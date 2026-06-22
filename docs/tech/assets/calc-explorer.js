/* PureScore Calculation Explorer — Flight-Deck cockpit for purescore-uber-map.html.
 * Instrument band (score · companion · 12 pillar dials · 15 reservoir tanks) + decision tree
 * (hover→companion, PRO questionnaires, data-state, completeness, last-captured, baselines) +
 * persistent execution-trace panel + bottom strip (nudges · adherence · forecast). All from
 * window.PURESCORE_DATA via window.PureScore (engine.js). Illustrative (README §5.6). */
(function () {
  "use strict";
  var D = window.PURESCORE_DATA, PS = window.PureScore;
  if (!D || !PS) { return; }

  function esc(s){ return String(s).replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;"); }
  function aColor(a){ var x=D.areas.filter(function(z){return z.k===a;})[0]; return x?x.c:"#7b8aa0"; }
  function js(o){ return JSON.stringify(o,null,1); }
  function zoneColor(z){ return z==="red"?"#f0606e":z==="yellow"?"#edc14a":z==="green"?"#3ad6a0":"#5b7790"; }
  function pct(x){ return Math.round(x*100)+"%"; }
  var DOCFILE={ "01":"01-vision-principles-and-lessons.html","06":"06-data-model-and-reference-ranges.html",
    "02":"02-pillars-and-marker-catalog.html","03":"03-scoring-formula.html","04":"04-moniac-reservoir-dynamics.html",
    "08":"08-sex-specific-models.html","09":"09-acute-events-and-life-stage-plans.html","10":"10-clinical-scores-integration.html",
    "19":"19-actuarial-pricing-and-insurance.html","05":"05-critical-review-and-purescore-2.0.html","07":"07-data-streams-and-experience.html" };
  function docHref(d){ return DOCFILE[d]||d; }
  function docLabel(d){ return /\.html$/.test(d)?d.replace(".html",""):("Doc "+d); }
  var NOW = (function(){ try { return Date.now(); } catch(e){ return 0; } })();

  var state={ prof:0, root:"purescore", mk:{}, st:{}, qresp:{}, R:null, view:"tree", sel:null, range:"30", baseR:null, baseMk:{}, baseSt:{}, softmaxT:0.15 };
  function deepClone(o){ return JSON.parse(JSON.stringify(o)); }
  function gammaVal(){ var c=D._constants_raw||[]; for(var i=0;i<c.length;i++){ if(c[i][0]==="γ") return parseFloat(c[i][2]); } return 3; }
  var CFG0=null;
  function snapConfig(){ CFG0={const:Object.assign({},D.const), weights:{}, coupling:deepClone(D.reservoir_coupling), softmaxT:state.softmaxT}; for(var pid in D.pillars) CFG0.weights[pid]=D.pillars[pid].weight; }
  function curProfile(){ return D.profiles[state.prof]; }
  function medSets(){ var managed={}, confound={}; (state.meds||[]).forEach(function(atc){ var m=D.conditions&&D.conditions.meds&&D.conditions.meds[atc]; if(!m) return; (m.manages||[]).forEach(function(x){managed[x]=1;}); (m.confounds||[]).forEach(function(x){confound[x]=1;}); }); return {managed:managed, confound:confound}; }
  function eopts(){ var p=curProfile(), ms=medSets(), adh=(p.adh!=null?p.adh:0.8)+(state.adhBoost||0); return {adh:Math.min(1,Math.max(0,adh)), preg:p.preg, sex:p.sex, age:p.age, lifestage:p.lifestage, st:state.st, managed:ms.managed, confound:ms.confound}; }
  function recompute(){ state.R=PS.score(state.mk, eopts()); }
  function markerVal(m){ return state.mk[m]!=null?state.mk[m]:D.markers[m].default; }

  /* ---------- per-marker provenance helpers ---------- */
  function capInfo(m){ var mm=D.markers[m], stt=state.st[m]||"present";
    if(stt==="missing") return {txt:"never captured", days:null};
    var base={wearable:1,lab:30,clinical:120,pro:14}[mm.source]||30;
    var days=(stt==="stale")?(D.engine_params.stale_dt_days||60):base;
    var d=new Date(NOW-days*86400000);
    return {txt:days+"d ago · "+(d.getMonth()+1)+"/"+d.getDate(), days:days};
  }
  function wearInfo(m){ var wm=D.wearmeta&&D.wearmeta[m]; if(!wm) return null; var mm=D.markers[m];
    var cur=markerVal(m), bse=mm.default; return {tier:wm.tier, acc:wm.accuracy, dev:wm.devices, base:bse, cur:cur, dpct:bse?Math.round((cur-bse)/bse*100):0}; }
  function ensureQ(m){ if(state.qresp[m]) return; var inst=D.instruments[m]; var tot=Math.round(markerVal(m));
    var n=inst.items.length, mx=inst.maxlvl, b=Math.floor(tot/n), rem=tot-b*n, arr=[];
    for(var i=0;i<n;i++){ arr.push(Math.min(mx, b+(i<rem?1:0))); } state.qresp[m]=arr; }
  function sumQ(m){ var a=state.qresp[m]; if(!a) return markerVal(m); var s=0; a.forEach(function(v){s+=v||0;}); return s; }

  /* ================= INSTRUMENT BAND ================= */
  var PILLEL={}, RESEL={};
  function renderBandShell(){
    var pg=document.getElementById("fdPillars"); pg.innerHTML=""; PILLEL={};
    Object.keys(D.pillars).forEach(function(pid){
      var el=document.createElement("div"); el.className="fd-pill"; el.dataset.pid=pid;
      var idrow=document.createElement("div"); idrow.className="id";
      var nm=document.createElement("span"); nm.textContent=pid.toUpperCase();
      var rv=document.createElement("span"); rv.className="r";
      idrow.appendChild(nm); idrow.appendChild(rv);
      var bar=document.createElement("div"); bar.className="bar"; var bi=document.createElement("i"); bar.appendChild(bi);
      el.appendChild(idrow); el.appendChild(bar); el._r=rv; el._bar=bi;
      el.onclick=function(){ traceCtx({pid:pid}, D.pillars[pid].label); };
      el.onmouseenter=function(ev){ showTip(ev,tipPillar(pid)); }; el.onmousemove=moveTip; el.onmouseleave=hideTip;
      pg.appendChild(el); PILLEL[pid]=el; });
    var rg=document.getElementById("fdRes"); rg.innerHTML=""; RESEL={};
    Object.keys(D.reservoirs).forEach(function(rid){ var rr=D.reservoirs[rid];
      var t=document.createElement("div"); t.className="fd-tank "+rr.polarity; t.dataset.rid=rid; t.title=rr.label;
      var fi=document.createElement("i"); t.appendChild(fi); t._fill=fi;
      t.onclick=function(){ traceCtx({pid:rr.feeds, rid:rid}, rr.label); };
      t.onmouseenter=function(ev){ showTip(ev,tipRes(rid)); }; t.onmousemove=moveTip; t.onmouseleave=hideTip;
      rg.appendChild(t); RESEL[rid]=t; });
  }
  function renderBand(){
    var R=state.R;
    document.getElementById("fdScoreN").textContent=R.score;
    var band=R.score>=80?"ON TRACK":R.score>=60?"BUILDING":R.score>=40?"AT RISK":"CRITICAL";
    var bb=document.getElementById("fdScoreB"); bb.textContent=band; bb.style.color=R.score>=80?"#3ad6a0":R.score>=60?"#edc14a":"#f0606e";
    var imp=0,meas=0; for(var pid in R.detail){ var ms=R.detail[pid].markers; for(var m in ms){ if(ms[m].imputed) imp++; else if(ms[m].included) meas++; } }
    document.getElementById("fdComp").innerHTML=
      gauge("Confidence", R.confidence.toFixed(2)) + gauge("Coverage", pct(R.coverage)+(R.lowCoverage?" ⚠":"")) +
      gauge("Trajectory", R.companion.trajectory) + gauge("Early-warn", R.companion.ew?"● ON":"○ off") +
      gauge("Provenance", meas+" meas · "+imp+" imp") + gauge("Critical", R.crit.length?("⚑ "+R.crit.join(",").toUpperCase()):"none");
    Object.keys(PILLEL).forEach(function(pid){ var el=PILLEL[pid], d=R.detail[pid], z=PS.pillarZone(d.R);
      el._r.textContent=d.R.toFixed(2); el._bar.style.width=Math.round(d.R*100)+"%"; el._bar.style.background=zoneColor(z);
      el.classList.toggle("crit",d.crit); el.classList.toggle("lowcov",d.lowCoverage); });
    Object.keys(RESEL).forEach(function(rid){ RESEL[rid]._fill.style.height=Math.round(R.res.coupled[rid]*100)+"%"; });
    renderScoreCI();
  }
  function renderScoreCI(){ var R=state.R, el=document.getElementById("fdScoreCI"); if(!el) return;
    var ci=Math.round((1-R.confidence)*16 + (1-R.coverage)*9);
    var anyImp=false; for(var p in R.detail){ var ms=R.detail[p].markers; for(var m in ms){ if(ms[m].imputed){ anyImp=true; break; } } }
    var prov=R.lowCoverage||anyImp||R.confidence<0.6;
    el.innerHTML="± "+ci+" · "+(prov?'<span class="prov">PROVISIONAL</span>':"measured");
    var band=document.getElementById("fdScoreBand"); if(band){ var lo=Math.max(0,R.score-ci), hi=Math.min(100,R.score+ci), w=84;
      band.innerHTML='<i style="left:'+(lo/100*w).toFixed(1)+'px;width:'+((hi-lo)/100*w).toFixed(1)+'px"></i><b style="left:'+(R.score/100*w).toFixed(1)+'px"></b>'; band.title=lo+"–"+hi+" (widens with missing data)"; } }
  function gauge(l,v){ return '<div class="fd-gauge">'+l+' <b>'+esc(String(v))+'</b></div>'; }

  /* ================= BOTTOM STRIP ================= */
  function hashStr(s){ var h=0; for(var i=0;i<s.length;i++){ h=(h*31+s.charCodeAt(i))>>>0; } return h; }
  function adhColor(v){ return v>=0.7?"#3ad6a0":v>=0.4?"#edc14a":"#f0606e"; }
  var MODIFIABLE={met:1,slp:1,fit:1,mcs:1,nut:1,bcm:1};
  function companionDims(){
    var R=state.R, modR=0,totR=0;
    for(var pid in D.pillars){ var c=D.pillars[pid].weight*R.detail[pid].R; totR+=c; if(MODIFIABLE[pid]) modR+=c; }
    var n=0,repw=0,meas=0,imp=0;
    for(var p in R.detail){ var ms=R.detail[p].markers; for(var m in ms){ n++; var s=ms[m].state;
      repw += (ms[m].imputed?D.const.rp_impute:(s==="stale"?0.8:(ms[m].included?1:0)));
      if(ms[m].imputed) imp++; else if(ms[m].included) meas++; } }
    return { confidence:R.confidence, coverage:R.coverage, modifiability: totR>0?modR/totR:0,
      representativeness: n?repw/n:0, provenance:(meas+imp)?meas/(meas+imp):0,
      trajectory:R.companion.trajectory, ew:R.companion.ew };
  }
  function renderStrip(){
    var R=state.R;
    document.getElementById("fdNudge").innerHTML=R.nudge.map(function(x){
      return '<div class="fd-nud"><span>'+(x.pillar.toUpperCase())+'</span><span class="small muted">U='+x.U+'</span><span class="d">+'+(x.impact*8).toFixed(1)+'</span></div>'; }).join("");
    document.getElementById("fdForecast").innerHTML=forecastSvg();
    // companion vector panel
    var cv=companionDims();
    function bar(l,v){ return '<div class="fd-cv"><span class="l">'+l+'</span><span class="t"><i style="width:'+Math.round(v*100)+'%;background:'+(v>=0.6?"#3ad6a0":v>=0.35?"#edc14a":"#f0606e")+'"></i></span><span class="v">'+Math.round(v*100)+'%</span></div>'; }
    document.getElementById("fdCompPanel").innerHTML=
      bar("Confidence",cv.confidence)+bar("Coverage",cv.coverage)+bar("Representv.",cv.representativeness)+
      bar("Provenance",cv.provenance)+bar("Modifiable",cv.modifiability)+
      '<div class="fd-cv"><span class="l">Trajectory</span><span class="v" style="width:auto;color:'+(/improv/.test(cv.trajectory)?"#3ad6a0":"#edc14a")+'">'+esc(cv.trajectory)+'</span></div>'+
      '<div class="fd-cv"><span class="l">Early-warn</span><span class="v" style="width:auto;color:'+(cv.ew?"#f0606e":"#6b7d92")+'">'+(cv.ew?"● ON":"○ off")+'</span></div>';
    // patient action queue (goals)
    document.getElementById("fdQueue").innerHTML=patientQueue().map(function(g){
      var pk=(g.pillar||"").toLowerCase(), d=R.rk[pk]!=null?(R.rk[pk]*8).toFixed(1):"–";
      return '<div class="fd-q"><span class="d">+'+d+'</span><span class="tag">'+esc(g.pillar||"")+'</span><span class="t">'+esc(g.title)+'</span>'
        +'<div class="m">'+esc(g.metric.name||"")+' → '+esc(g.metric.target||"")+' · '+(g.metric.horizon_wk||"?")+'wk</div></div>'; }).join("")
      || '<div class="small muted">no applicable goals for this profile</div>';
    // adherence check-ins to ask
    document.getElementById("fdCheckins").innerHTML=checkinItems().map(function(it){
      return '<div class="fd-ci"><div class="s">'+esc(it.stem||"")+'</div><div class="o">'+(it.responses||[]).map(function(r){ return '<span>'+esc(r.label)+'</span>'; }).join("")+'</div></div>'; }).join("")
      || '<div class="small muted">no check-ins for the focus pillars</div>';
    // adherence history
    document.getElementById("fdAdhHist").innerHTML=adhHistoryHtml();
    renderWaterfall(); renderSystems(); renderWhatif(); renderCrit(); renderTrend(); renderWearBaselines();
    renderDxMeds(); renderQuestions(); renderAdhActions();
  }
  /* ---- wearable baselines (robust stats + drill-down, after wearable-baselines.html) ---- */
  function meanOf(a){ var s=0; a.forEach(function(v){s+=v;}); return a.length?s/a.length:0; }
  function ewmaOf(a,al){ var e=a[0]; for(var i=1;i<a.length;i++) e=al*a[i]+(1-al)*e; return e; }
  function madOf(a,med){ var d=a.map(function(v){return Math.abs(v-med);}).sort(function(x,y){return x-y;}); return d[Math.floor(d.length/2)]; }
  function sig1(x){ return (x>=0?"+":"")+x.toFixed(1); }
  function fmtv(v,dec){ var p=Math.pow(10,dec||0); return (Math.round(v*p)/p).toFixed(dec||0); }
  function zColor(z){ var a=Math.abs(z); return a<1?"#3ad6a0":a<2?"#edc14a":"#f0606e"; }
  function ownerPillarOf(m){ for(var pid in D.pillars){ if(D.pillars[pid].markers.indexOf(m)>=0) return pid; } return Object.keys(D.pillars)[0]; }
  function miniSpark(ser,color,big){ var w=big?260:60,h=big?32:16,mn=Math.min.apply(null,ser),mx=Math.max.apply(null,ser),rg=(mx-mn)||1;
    var sx=function(i){return i/(ser.length-1)*w;}, sy=function(v){return h-((v-mn)/rg)*(h-3)-1.5;};
    var d=""; for(var i=0;i<ser.length;i++) d+=(i?"L":"M")+sx(i).toFixed(1)+" "+sy(ser[i]).toFixed(1)+" ";
    return '<svg width="'+(big?"100%":w)+'" height="'+h+'"'+(big?' viewBox="0 0 '+w+' '+h+'" preserveAspectRatio="none"':'')+'><path d="'+d+'" fill="none" stroke="'+color+'" stroke-width="'+(big?2:1.4)+'"/></svg>'; }
  function miniSparkAnom(s){ var ser=s.ser,med=s.median,sig=s.sigma,w=260,h=34,mn=Math.min.apply(null,ser),mx=Math.max.apply(null,ser),rg=(mx-mn)||1;
    var sx=function(i){return i/(ser.length-1)*w;}, sy=function(v){return h-((v-mn)/rg)*(h-4)-2;};
    var d="",dots=""; for(var i=0;i<ser.length;i++){ d+=(i?"L":"M")+sx(i).toFixed(1)+" "+sy(ser[i]).toFixed(1)+" "; if(Math.abs((ser[i]-med)/sig)>2) dots+='<circle cx="'+sx(i).toFixed(1)+'" cy="'+sy(ser[i]).toFixed(1)+'" r="2.5" fill="#f0606e"/>'; }
    return '<svg width="100%" height="'+h+'" viewBox="0 0 '+w+' '+h+'" preserveAspectRatio="none"><path d="'+d+'" fill="none" stroke="'+zColor(s.z)+'" stroke-width="2"/>'+dots+'</svg>'; }
  function pillarTrendHtml(pid){ var n=(state.range==="7"?7:state.range==="90"?90:30), s=trendSeries(pid,n);
    return '<div class="fd-k">'+pid.toUpperCase()+' trend · health 1−R · '+n+'d</div>'+miniSpark(s,"#49c6d8",true); }
  function wbSeries(m){ var mm=D.markers[m], N=30, today=markerVal(m), basec=mm.default, seed=hashStr(m+state.prof)%101;
    var amp=Math.abs(basec)*0.05 + (mm.th?Math.abs(mm.th[0]-mm.th[1])*0.05:0.5)+1e-3, ser=[];
    for(var i=0;i<N;i++){ var t=i/(N-1), center=basec+(today-basec)*Math.pow(t,2.2), noise=amp*Math.sin(seed*0.7+i*0.6)+amp*0.5*Math.cos(seed*0.3+i*1.3); ser.push(center+noise); }
    ser[N-1]=today;
    var sorted=ser.slice().sort(function(a,b){return a-b;}), median=sorted[Math.floor(N/2)], sigma=Math.max(1e-6,1.4826*madOf(ser,median));
    var a7=meanOf(ser.slice(-7)), a28=meanOf(ser), z=(today-median)/sigma;
    var inb=ser.filter(function(v){return Math.abs((v-median)/sigma)<1;}).length, anom=ser.filter(function(v){return Math.abs((v-median)/sigma)>2;}).length;
    var stt=state.st[m]||"present", cov=stt==="missing"?0:(stt==="stale"?0.5:(27+seed%4)/30);
    return {N:N,ser:ser,today:today,median:median,sigma:sigma,a7:a7,a28:a28,ewma:ewmaOf(ser,0.3),z:z,acwr:a28?a7/a28:1,inb:inb/N,anom:anom,cov:Math.min(1,cov),unit:mm.unit,dec:Math.abs(basec)<10?1:0}; }
  function renderWearBaselines(){ var host=document.getElementById("fdWearBase"); if(!host) return;
    var wm=Object.keys(D.markers).filter(function(m){return D.markers[m].source==="wearable";});
    host.innerHTML=wm.map(function(m){ var mm=D.markers[m], stt=state.st[m]||"present";
      if(stt==="missing") return '<div class="fd-wb off" data-m="'+m+'"><span class="nm">'+esc(mm.label)+'</span><span class="meta">no device</span></div>';
      var s=wbSeries(m);
      return '<div class="fd-wb" data-m="'+m+'"><span class="nm">'+esc(mm.label)+'</span>'+miniSpark(s.ser,zColor(s.z))
        +'<span class="tv">'+fmtv(s.today,s.dec)+'</span><span class="zc" style="color:'+zColor(s.z)+'">'+sig1(s.z)+'σ</span>'
        +(s.anom?' <span style="color:#f0606e">'+s.anom+'⚠</span>':'')+(stt==="stale"?' <span style="color:#edc14a">stale</span>':'')+'</div>'; }).join("");
    [].forEach.call(host.querySelectorAll(".fd-wb"),function(el){ if(el.classList.contains("off")) return; el.onclick=function(){ var m=el.getAttribute("data-m"); traceCtx({pid:ownerPillarOf(m),mid:m}, D.markers[m].label+" — wearable baseline"); }; }); }
  function wbDetailHtml(m){ var s=wbSeries(m), mm=D.markers[m], lo=s.median-2*s.sigma, hi=s.median+2*s.sigma, pos=Math.max(0,Math.min(1,(s.today-lo)/((hi-lo)||1)));
    var band='<div class="fd-band-viz"><div class="b1"></div><div class="mk" style="left:'+(pos*100).toFixed(1)+'%"></div></div>';
    var trend=s.a7>s.a28?"▲":(s.a7<s.a28?"▼":"▶"), stt=state.st[m]||"present";
    function c(l,v){ return '<span class="ce-chip">'+l+' '+v+'</span>'; }
    var note='<div class="small muted" style="margin-top:4px">'+(stt==="stale"?"⚠ stale — confidence decayed; baseline from last sync. ":"")+(s.anom?('<span style="color:#f0606e">'+s.anom+" anomaly point"+(s.anom>1?"s":"")+" (&gt;2σ) in 30d</span>"):"no anomalies in 30d")+"</div>";
    return '<div class="fd-k">wearable baseline · 28-day (median ± σ) · red = anomaly</div>'+miniSparkAnom(s)+band+note
      +'<div class="ce-chips" style="margin-top:6px">'+c("today",fmtv(s.today,s.dec)+(mm.unit?(" "+mm.unit):""))+c("baseline",fmtv(s.median,s.dec)+" ±"+fmtv(s.sigma,s.dec))
      +c("z",sig1(s.z)+"σ")+c("EWMA",fmtv(s.ewma,s.dec))+c("7d vs 28d",trend+" "+fmtv(s.a7,s.dec)+"→"+fmtv(s.a28,s.dec))
      +c("Acute:Chronic",s.acwr.toFixed(2))+c("% in band 30d",Math.round(s.inb*100)+"%")+c("anomalies 30d",s.anom)+c("coverage 30d",Math.round(s.cov*100)+"%")+'</div>'; }
  /* ---- config / weights bar (session what-if; reset to JSON canonical) ---- */
  function fmtKnob(v){ return Math.abs(v)>=10?String(Math.round(v)):(Math.round(v*100)/100).toFixed(2); }
  function headChips(){ var C=D.const, wsum=0; for(var pid in D.pillars) wsum+=D.pillars[pid].weight;
    function k(l,v){ return '<span class="k">'+l+' <b>'+v+'</b></span>'; }
    return k("δ",C.delta)+k("ρ",C.rho)+k("φ",C.phi)+k("γ",gammaVal())+k("R_crit",C.r_crit)+k("cap",C.pure_crit_cap)+k("softmaxT",fmtKnob(state.softmaxT))+k("q_imp",C.q_impute)+k("Rp",C.rp_impute)+k("cov_floor",C.cov_green_floor)+k("ΣW",wsum.toFixed(2)); }
  function knob(key,label,mn,mx,st,val){ return '<div class="fd-knob" data-key="'+key+'"><label>'+esc(label)+'</label><input type="range" min="'+mn+'" max="'+mx+'" step="'+st+'" value="'+val+'"><span class="vv">'+fmtKnob(val)+'</span></div>'; }
  function knobsHtml(){ var C=D.const;
    var consts=[["delta","δ",1,4,1],["rho","ρ_k",0,0.5,0.01],["phi","φ",0,1,0.05],["r_crit","R_crit",0,1,0.05],["pure_crit_cap","PURE_CRIT_CAP",0,100,1],["q_impute","q_impute",0,1,0.05],["rp_impute","Rp_impute",0,1,0.05],["cov_green_floor","cov_floor",0,1,0.05],["softmaxT","softmax T",0.02,0.6,0.01]];
    var ch=consts.map(function(c){ var v=(c[0]==="softmaxT")?state.softmaxT:C[c[0]]; return knob("c:"+c[0],c[1],c[2],c[3],c[4],v); }).join("");
    var pw=Object.keys(D.pillars).map(function(pid){ return knob("w:"+pid,pid.toUpperCase(),0,0.30,0.005,D.pillars[pid].weight); }).join("");
    var inputs=Object.keys(D.reservoirs).map(function(rid){ var rr=D.reservoirs[rid]; return '<div class="fd-cfg-res"><b>'+rid.toUpperCase()+'</b> = mean('+rr.inputs.map(function(i){return i[0]+"["+i[1]+"→"+i[2]+"]";}).join(" · ")+') · '+rr.polarity+'</div>'; }).join("");
    var resk=[]; D.reservoir_coupling.forEach(function(cp,i){ (cp.add||[]).forEach(function(a){ resk.push(knob("k:"+i+":a:"+a[0],cp.t.toUpperCase()+"←"+a[0],0,0.5,0.01,a[1])); }); (cp.subDeficit||[]).forEach(function(a){ resk.push(knob("k:"+i+":s:"+a[0],cp.t.toUpperCase()+"⊖"+a[0],0,0.5,0.01,a[1])); }); });
    return '<div class="fd-cfg-sub">constants &amp; softmax (Doc 03 / 04 · constants.json)</div><div class="fd-cfg-grid">'+ch+'</div>'
      +'<div class="fd-cfg-sub">pillar weights · engine normalises by ΣW (pillar-weights.json)</div><div class="fd-cfg-grid">'+pw+'</div>'
      +'<div class="fd-cfg-sub">reservoir multivariate inputs (read-only)</div>'+inputs
      +'<div class="fd-cfg-sub">reservoir coupling weights (Doc 04)</div><div class="fd-cfg-grid">'+resk.join("")+'</div>'
      +'<div class="fd-cfg-actions"><button class="um-btn" id="fdCfgReset">reset to JSON canonical</button><span class="fd-cfg-warn" id="fdCfgWarn"></span></div>'; }
  function renderConfig(){ var host=document.getElementById("fdConfig"); if(!host) return; if(CFG0===null) snapConfig();
    var open=host.classList.contains("open");
    host.innerHTML='<div class="fd-cfg-head" id="fdCfgHead"><span id="fdCfgChips">'+headChips()+'</span><span class="sp"></span><span class="ed">'+(open?"− collapse":"✎ edit knobs")+'</span></div><div class="fd-cfg-body">'+knobsHtml()+'</div>';
    var head=document.getElementById("fdCfgHead"); if(head) head.onclick=function(){ host.classList.toggle("open"); renderConfig(); };
    wireKnobs(); }
  function wireKnobs(){ [].forEach.call(document.querySelectorAll("#fdConfig .fd-knob input"),function(inp){ inp.oninput=function(){ var key=inp.parentNode.getAttribute("data-key"); applyKnob(key,parseFloat(inp.value)); inp.parentNode.querySelector(".vv").textContent=fmtKnob(parseFloat(inp.value)); }; });
    var r=document.getElementById("fdCfgReset"); if(r) r.onclick=resetConfig; }
  function applyKnob(key,val){ var p=key.split(":");
    if(p[0]==="c"){ if(p[1]==="softmaxT") state.softmaxT=val; else D.const[p[1]]=val; }
    else if(p[0]==="w"){ D.pillars[p[1]].weight=val; }
    else if(p[0]==="k"){ var cp=D.reservoir_coupling[+p[1]], list=p[2]==="a"?cp.add:cp.subDeficit; for(var j=0;j<list.length;j++){ if(list[j][0]===p[3]){ list[j][1]=val; break; } } }
    var warn=document.getElementById("fdCfgWarn"); if(warn) warn.textContent="session what-if — diverges from JSON canonical";
    recompute(); computeBaseline(); refreshAll(); var ch=document.getElementById("fdCfgChips"); if(ch) ch.innerHTML=headChips(); }
  function resetConfig(){ for(var k in CFG0.const) D.const[k]=CFG0.const[k]; for(var pid in CFG0.weights) D.pillars[pid].weight=CFG0.weights[pid]; D.reservoir_coupling=deepClone(CFG0.coupling); state.softmaxT=CFG0.softmaxT; recompute(); computeBaseline(); refreshAll(); renderConfig(); }

  /* ---- diagnoses & meds (Patient360) + simulate EHR ---- */
  function renderDxMeds(){ var host=document.getElementById("fdDxMeds"); if(!host||!D.conditions) return; var dis=D.conditions.diseases||{}, meds=D.conditions.meds||{};
    var dxh=(state.dx||[]).map(function(c){ var d=dis[c]; return '<span class="fd-dx" data-dx="'+c+'">'+esc(d?d.name:c)+'<span class="c">'+c+'</span></span>'; }).join("")||'<span class="small muted">none recorded</span>';
    var mdh=(state.meds||[]).map(function(c){ var m=meds[c]; return '<span class="fd-dx med" data-med="'+c+'">'+esc(m?m.name:c)+'<span class="c">'+c+'</span></span>'; }).join("")||'<span class="small muted">none</span>';
    host.innerHTML='<div class="fd-qsub">diagnoses (ICD-10)</div>'+dxh+'<div class="fd-qsub">medications (ATC)</div>'+mdh;
    [].forEach.call(host.querySelectorAll("[data-dx]"),function(el){ el.onclick=function(){ traceDisease(el.getAttribute("data-dx")); }; });
    [].forEach.call(host.querySelectorAll("[data-med]"),function(el){ el.onclick=function(){ traceMed(el.getAttribute("data-med")); }; }); }
  function traceDisease(icd){ var d=D.conditions.diseases[icd]; if(!d) return; selectRow(null); var b=document.getElementById("ceTraceBody"), p=[];
    p.push('<b>'+esc(d.name)+'</b> <span class="ce-chip">'+icd+'</span>');
    p.push('<div class="fd-k">associated conditions / comorbidities</div><div class="ce-chips">'+(d.comorbid||[]).map(function(x){return '<span class="ce-chip">'+esc(x)+'</span>';}).join(" ")+'</div>');
    p.push('<div class="fd-k">affected pillars (click to trace)</div><div class="ce-chips">'+(d.pillars||[]).map(function(pid){return '<a class="ce-chip" data-gp="'+pid+'">'+pid.toUpperCase()+' R='+(state.R.detail[pid]?state.R.detail[pid].R.toFixed(2):"?")+'</a>';}).join(" ")+'</div>');
    p.push('<div class="fd-k">affected markers</div><div class="ce-chips">'+(d.markers||[]).map(function(m){return '<span class="ce-chip">'+esc(D.markers[m]?D.markers[m].label:m)+'</span>';}).join(" ")+'</div>');
    if(d.meds&&d.meds.length) p.push('<div class="fd-k">typical medications</div><div class="ce-chips">'+d.meds.map(function(a){return '<span class="ce-chip">'+esc(D.conditions.meds[a]?D.conditions.meds[a].name:a)+'</span>';}).join(" ")+'</div>');
    b.innerHTML=p.join(""); [].forEach.call(b.querySelectorAll("[data-gp]"),function(el){ el.onclick=function(){ var pid=el.getAttribute("data-gp"); traceCtx({pid:pid}, D.pillars[pid].label); }; }); }
  function traceMed(atc){ var m=D.conditions.meds[atc]; if(!m) return; selectRow(null); var b=document.getElementById("ceTraceBody"), p=[];
    p.push('<b>'+esc(m.name)+'</b> <span class="ce-chip">'+atc+'</span> <span class="ce-chip">'+esc(m.class||"")+'</span>');
    p.push('<div class="fd-k">manages — controlled shows green but tagged (D3)</div><div class="ce-chips">'+((m.manages||[]).map(function(x){return '<span class="ce-chip">'+esc(D.markers[x]?D.markers[x].label:x)+'</span>';}).join(" ")||'<span class="small muted">—</span>')+'</div>');
    p.push('<div class="fd-k">confounds — confidence down-weighted (D3)</div><div class="ce-chips">'+((m.confounds&&m.confounds.length)?m.confounds.map(function(x){return '<span class="ce-chip">'+esc(D.markers[x]?D.markers[x].label:x)+'</span>';}).join(" "):'<span class="small muted">none</span>')+'</div>');
    b.innerHTML=p.join(""); }
  function populateSim(){ var ty=document.getElementById("fdSimType"), sel=document.getElementById("fdSimItem"); if(!ty||!sel) return; var t=ty.value, o="";
    if(t==="lab"){ for(var m in D.markers){ if(D.markers[m].source==="lab" && (state.st[m]==="missing")) o+='<option value="'+m+'">'+esc(D.markers[m].label)+'</option>'; } if(!o) o='<option value="">(no missing labs)</option>'; }
    else if(t==="dx"){ for(var c in D.conditions.diseases){ if((state.dx||[]).indexOf(c)<0) o+='<option value="'+c+'">'+c+" · "+esc(D.conditions.diseases[c].name)+'</option>'; } }
    else { for(var a in D.conditions.meds){ if((state.meds||[]).indexOf(a)<0) o+='<option value="'+a+'">'+a+" · "+esc(D.conditions.meds[a].name)+'</option>'; } }
    sel.innerHTML=o; }
  function simAdd(){ var t=document.getElementById("fdSimType").value, v=document.getElementById("fdSimItem").value; if(!v) return;
    if(t==="lab"){ delete state.st[v]; if(state.mk[v]==null) state.mk[v]=D.markers[v].default; }
    else if(t==="dx"){ if(state.dx.indexOf(v)<0) state.dx.push(v); (D.conditions.diseases[v].markers||[]).forEach(function(m){ if(state.st[m]==="missing") delete state.st[m]; }); }
    else { if(state.meds.indexOf(v)<0) state.meds.push(v); }
    recompute(); refreshAll(); renderDxMeds(); populateSim(); }
  /* ---- onboarding & periodic questions answered ---- */
  function renderQuestions(){ var host=document.getElementById("fdQuestions"); if(!host||!D.questions) return; var p=curProfile();
    function applies(q){ var sx=q.sex||["all"]; return (sx.indexOf("all")>=0||sx.indexOf(p.sex)>=0) && p.age>=q.amin && p.age<=q.amax; }
    function pick(q){ if(!q.responses.length) return null; var risky=(q.pillars||[]).some(function(pk){ return state.R.rk[pk]>0.4; }); var idx=risky?(q.responses.length-1):(hashStr(q.id+state.prof)%q.responses.length); return q.responses[Math.min(idx,q.responses.length-1)]; }
    function row(q){ var r=pick(q); if(!r) return ""; return '<div class="fd-q2" data-pills="'+(r.p||[]).join(",")+'"><span class="qt">'+esc(q.text)+'</span><span class="qa">'+esc(r.l||"")+'</span><span class="qp">'+(r.p||[]).map(function(x){return x.toUpperCase();}).join(",")+'</span></div>'; }
    var elig=D.questions.filter(applies);
    var onb=elig.filter(function(q){return q.cadence==="Core"||q.cadence==="once";}).slice(0,8);
    var per=elig.filter(function(q){return q.cadence==="quarterly"||q.cadence==="annual";}).slice(0,8);
    host.innerHTML='<div class="fd-qsub">onboarding intake ('+onb.length+')</div>'+onb.map(row).join("")+'<div class="fd-qsub">periodic ('+per.length+')</div>'+per.map(row).join("");
    [].forEach.call(host.querySelectorAll(".fd-q2"),function(el){ el.onclick=function(){ var pi=(el.getAttribute("data-pills")||"").split(",").filter(Boolean)[0]; if(pi&&D.pillars[pi]) traceCtx({pid:pi}, D.pillars[pi].label); }; }); }
  /* ---- improve adherence (barrier-matched + generic) ---- */
  function profileBarriers(){ var all=Object.keys((D.adherence_actions&&D.adherence_actions.barriers)||{}); if(!all.length) return []; var seed=hashStr("bar"+state.prof), n=2+seed%2, out=[]; for(var i=0;i<n;i++){ var b=all[(seed+i*3)%all.length]; if(out.indexOf(b)<0) out.push(b); } return out; }
  function renderAdhActions(){ var host=document.getElementById("fdAdhActions"); if(!host||!D.adherence_actions) return; var aa=D.adherence_actions, rows=[];
    profileBarriers().forEach(function(b){ (aa.barriers[b]||[]).slice(0,2).forEach(function(act){ rows.push({b:b,act:act}); }); });
    (aa.generic||[]).slice(0,2).forEach(function(act){ rows.push({b:"generic",act:act}); });
    host.innerHTML='<div class="small muted" style="margin-bottom:3px">adherence EWMA '+state.R.adherence.adherence.toFixed(2)+(state.adhBoost?(' (+'+Math.round(state.adhBoost*100)+'% applied)'):"")+'</div>'
      +rows.slice(0,7).map(function(x){ var key=x.b+"|"+x.act.label, done=state.appliedAdh[key];
        return '<div class="fd-aa"><span class="bar'+(x.b==="generic"?" gen":"")+'">'+esc(x.b)+'</span><span class="t">'+esc(x.act.label)+'</span><span class="lift">+'+Math.round(x.act.lift*100)+'%</span><button data-k="'+esc(key)+'" data-l="'+x.act.lift+'" class="'+(done?"done":"")+'">'+(done?"applied":"apply")+'</button></div>'; }).join("");
    [].forEach.call(host.querySelectorAll(".fd-aa button"),function(btn){ btn.onclick=function(){ var k=btn.getAttribute("data-k"); if(state.appliedAdh[k]) return; state.appliedAdh[k]=1; state.adhBoost=(state.adhBoost||0)+parseFloat(btn.getAttribute("data-l")); recompute(); refreshAll(); }; }); }
  function renderWaterfall(){ var R=state.R, host=document.getElementById("fdWaterfall"); if(!host) return;
    var arr=Object.keys(D.pillars).map(function(pid){ return {pid:pid, c:D.pillars[pid].weight*Math.pow(R.detail[pid].R,D.const.delta)}; });
    var tot=arr.reduce(function(s,x){return s+x.c;},0)||1, lost=Math.max(0,100-R.score);
    arr.sort(function(a,b){return b.c-a.c;}); var maxc=arr[0].c||1;
    host.innerHTML=arr.slice(0,8).map(function(x){ var z=PS.pillarZone(R.detail[x.pid].R);
      return '<div class="fd-wf" data-pid="'+x.pid+'"><span class="id">'+x.pid.toUpperCase()+'</span><span class="t"><i style="width:'+Math.round(x.c/maxc*100)+'%;background:'+zoneColor(z)+'"></i></span><span class="v">'+(x.c/tot*lost).toFixed(1)+'</span></div>'; }).join("");
    [].forEach.call(host.querySelectorAll(".fd-wf"),function(el){ el.onclick=function(){ var pid=el.getAttribute("data-pid"); traceCtx({pid:pid}, D.pillars[pid].label); }; }); }
  function renderSystems(){ var host=document.getElementById("fdSystems"); if(!host) return;
    var chans=[["lab","labs"],["wearable","wearable"],["pro","PRO"],["clinical","clinical"]];
    host.innerHTML=chans.map(function(c){ var src=c[0],tot=0,pres=0,minD=null;
      for(var m in D.markers){ if(D.markers[m].source===src){ tot++; var stt=state.st[m]||"present"; if(stt!=="missing"){ pres++; var d={wearable:1,lab:30,clinical:120,pro:14}[src]; if(stt==="stale")d=D.engine_params.stale_dt_days; if(minD==null||d<minD)minD=d; } } }
      var ok=pres===tot&&tot>0, none=pres===0, col=ok?"#3ad6a0":none?"#f0606e":"#edc14a", icon=ok?"✓":none?"✗":"⚠";
      return '<div class="fd-sys"><span class="ch">'+c[1]+'</span><span class="st" style="color:'+col+'">'+icon+'</span><span class="meta">'+(minD!=null?(minD+"d sync"):"no data")+" · "+pres+"/"+tot+'</span></div>'; }).join(""); }
  function renderWhatif(){ var R=state.R, host=document.getElementById("fdWhatif"); if(!host) return;
    var p=curProfile(), opts={adh:p.adh,preg:p.preg,sex:p.sex,age:p.age,lifestage:p.lifestage};
    var changes=[]; for(var m in D.markers){ var cv=(state.mk[m]===undefined?null:state.mk[m]), bv=(state.baseMk[m]===undefined?null:state.baseMk[m]); var cs=state.st[m]||"present", bs=state.baseSt[m]||"present"; if(cv!==bv||cs!==bs) changes.push(m); }
    if(!changes.length){ host.innerHTML='<div class="small muted">No edits yet. Drag a slider, answer a questionnaire, or change a data-state to compare vs this profile&rsquo;s baseline.</div>'; return; }
    var rows=changes.slice(0,8).map(function(m){ var mk2=Object.assign({},state.mk), st2=Object.assign({},state.st);
      if(state.baseMk[m]===undefined) delete mk2[m]; else mk2[m]=state.baseMk[m];
      if((state.baseSt[m]||"present")==="present") delete st2[m]; else st2[m]=state.baseSt[m];
      var d=R.score-PS.score(mk2,Object.assign({},opts,{st:st2})).score, mm=D.markers[m];
      var disp=(state.st[m]==="missing")?"missing":((state.mk[m]!=null?state.mk[m]:mm.default)+(state.st[m]==="stale"?" (stale)":""));
      return '<div class="fd-wi"><span class="m">'+esc(mm.label)+' → '+esc(String(disp))+'</span><span class="d" style="color:'+(d>=0?"#3ad6a0":"#f0606e")+'">'+(d>=0?"+":"")+d+'</span></div>'; }).join("");
    var net=R.score-state.baseR.score;
    host.innerHTML=rows+'<div class="fd-wi net"><span class="m">net vs baseline</span><span class="d" style="color:'+(net>=0?"#3ad6a0":"#f0606e")+'">'+(net>=0?"+":"")+net+' ('+state.baseR.score+'→'+R.score+')</span></div>'; }
  function renderCrit(){ var R=state.R, host=document.getElementById("fdCrit"); if(!host) return;
    if(!R.crit.length){ host.innerHTML='<div class="fd-ok">✓ no pillar critical · PureScore not capped</div>'; return; }
    var uncapped=Math.round(100*(1-R.Rtot)), T=state.softmaxT;
    var html=R.crit.map(function(pid){ var d=R.detail[pid], ms=d.markers;
      var trig=Object.keys(ms).filter(function(m){ return ms[m].critical&&ms[m].state==="present"&&ms[m].r!=null&&ms[m].r>=0.50; });
      var items=Object.keys(ms).filter(function(m){return ms[m].r!=null;});
      var exps=items.map(function(m){return Math.exp(ms[m].r/T);}), se=exps.reduce(function(a,b){return a+b;},0)||1;
      var dom=items.map(function(m,i){return {m:m,w:exps[i]/se};}).sort(function(a,b){return b.w-a.w;})[0];
      var tg=trig.map(function(m){ return D.markers[m].label+" "+ms[m].v+D.markers[m].unit+" (r="+ms[m].r.toFixed(2)+")"; }).join(", ");
      return '<div class="fd-crit"><div class="h">⚑ '+pid.toUpperCase()+' critical</div>'
        +'<div class="x">'+esc(tg||"(critical marker red)")+' → R_k floored to R_crit '+D.const.r_crit+' (pre-floor '+(d.Rm+D.const.rho*d.bt).toFixed(2)+')</div>'
        +'<div class="sm">softmax dominance (illustrative, T='+T+'): <b>'+(dom?esc(D.markers[dom.m].label)+" "+Math.round(dom.w*100)+"%":"–")+'</b> of the pillar&rsquo;s risk attention</div></div>'; }).join("");
    html+='<div class="fd-crit" style="border-color:#7a5f24;background:#1a160c"><div class="h" style="color:#edc14a">PureScore sudden impact</div><div class="x">uncapped '+uncapped+' &rarr; capped <b>'+R.score+'</b> (PURE_CRIT_CAP '+D.const.pure_crit_cap+') · drop '+(uncapped-R.score)+'</div></div>';
    host.innerHTML=html; }
  function trendSeries(id,n){ var cur=id==="score"?state.R.score/100:(1-state.R.rk[id]); var seed=hashStr(id+state.prof)%97, arr=[];
    for(var i=0;i<n;i++){ var t=i/(n-1), base=cur-(1-t)*0.06*Math.cos(seed*0.3), drift=(1-t)*0.10*Math.sin(seed+i*0.4); arr.push(Math.max(0.02,Math.min(1,base+drift))); }
    arr[n-1]=cur; return arr; }
  function renderTrend(){ var host=document.getElementById("fdTrend"); if(!host) return;
    var range=state.range||"30", n=range==="7"?7:range==="30"?30:90, W=720,H=120,pad=4;
    var sx=function(i){return pad+i/(n-1)*(W-2*pad);}, sy=function(v){return H-pad-v*(H-2*pad);};
    var cols=["#49c6d8","#edc14a","#f0606e","#3ad6a0"];
    var pills=Object.keys(D.pillars).sort(function(a,b){return state.R.rk[b]-state.R.rk[a];}).slice(0,4);
    var lines=[{id:"score",col:"#fff",w:2.5}]; pills.forEach(function(pid,i){ lines.push({id:pid,col:cols[i],w:1.4}); });
    var svg='<svg width="100%" height="'+H+'" viewBox="0 0 '+W+' '+H+'" preserveAspectRatio="none">';
    // confidence ribbon around PureScore: wide in the sparse past, converging toward today
    var sser=trendSeries("score",n), ci0=((1-state.R.confidence)*0.15+0.04), rib="";
    for(var i=0;i<n;i++){ var cf=ci0*(1-0.7*i/(n-1)); rib+=(i?"L":"M")+sx(i).toFixed(1)+" "+sy(Math.min(1,sser[i]+cf)).toFixed(1)+" "; }
    for(var j=n-1;j>=0;j--){ var cf2=ci0*(1-0.7*j/(n-1)); rib+="L"+sx(j).toFixed(1)+" "+sy(Math.max(0,sser[j]-cf2)).toFixed(1)+" "; }
    svg+='<path d="'+rib+'Z" fill="rgba(73,198,216,.16)" stroke="none"/>';
    lines.forEach(function(ln){ var s=trendSeries(ln.id,n), d=""; for(var i=0;i<n;i++){ d+=(i?"L":"M")+sx(i).toFixed(1)+" "+sy(s[i]).toFixed(1)+" "; } svg+='<path d="'+d+'" fill="none" stroke="'+ln.col+'" stroke-width="'+ln.w+'"/>'; });
    svg+='</svg>';
    var legend='<div class="fd-legend"><span><i style="background:#fff"></i>PureScore</span>'+pills.map(function(pid,i){return '<span><i style="background:'+cols[i]+'"></i>'+pid.toUpperCase()+'</span>';}).join("")+'</div>';
    host.innerHTML=svg+legend;
    var rb=document.getElementById("fdRangeBtns"); if(rb){ rb.innerHTML=[["7","7d"],["30","30d"],["90","3mo"]].map(function(r){return '<button data-r="'+r[0]+'" class="'+(range===r[0]?"on":"")+'">'+r[1]+'</button>';}).join("");
      [].forEach.call(rb.querySelectorAll("button"),function(b){ b.onclick=function(){ state.range=b.getAttribute("data-r"); renderTrend(); }; }); } }
  function patientQueue(){ var R=state.R, p=curProfile();
    var elig=D.goals.filter(function(g){ var a=g.app; var sexok=(a.sex==="any"||a.sex===p.sex); var ageok=(p.age>=a.age_range[0]&&p.age<=a.age_range[1]); return sexok&&ageok; });
    elig.sort(function(g1,g2){ return (R.rk[(g2.pillar||"").toLowerCase()]||0)-(R.rk[(g1.pillar||"").toLowerCase()]||0); });
    var seen={}, out=[]; for(var i=0;i<elig.length&&out.length<5;i++){ var k=elig[i].pillar+elig[i].title; if(!seen[k]){ seen[k]=1; out.push(elig[i]); } } return out;
  }
  function checkinItems(){ var R=state.R; var focus=(R.crit.length?R.crit:Object.keys(R.rk).sort(function(a,b){return R.rk[b]-R.rk[a];}).slice(0,3)).map(function(x){return x.toUpperCase();});
    return D.adherence_items.filter(function(it){ return (it.pillars||[]).some(function(pl){ return focus.indexOf(pl)>=0; }); }).slice(0,5);
  }
  function adhHistoryHtml(){ var base=state.R.adherence.adherence, seed=state.prof+1, wk=[];
    for(var w=0;w<12;w++){ var v=base+0.18*Math.sin((w+seed)*0.8)-0.05*((w+seed)%4); wk.push(Math.max(0.05,Math.min(1,v))); }
    var w=210,h=40,mn=0,mx=1, sx=function(i){return i/(wk.length-1)*w;}, sy=function(v){return h-v*(h-6)-3;};
    var d=""; for(var i=0;i<wk.length;i++){ d+=(i?"L":"M")+sx(i).toFixed(1)+" "+sy(wk[i]).toFixed(1)+" "; }
    var spark='<svg width="100%" height="'+h+'" viewBox="0 0 '+w+' '+h+'" preserveAspectRatio="none"><path d="'+d+'" fill="none" stroke="#49c6d8" stroke-width="2"/></svg>';
    var dots='<div class="fd-hist-wk">'+wk.map(function(v){ return '<i style="background:'+adhColor(v)+'" title="'+v.toFixed(2)+'"></i>'; }).join("")+'</div>';
    var rows=checkinItems().slice(0,3).map(function(it){ var hs=hashStr(it.ref||it.family||""); var cells="";
      for(var c=0;c<8;c++){ var v=Math.max(0,Math.min(1, base+0.25*Math.sin(c+hs%7)-0.06*((c+hs)%3))); cells+='<i style="background:'+adhColor(v)+'"></i>'; }
      return '<div class="fd-hrow"><span class="nm">'+esc((it.family||it.ref||"").slice(0,14))+'</span><span class="cells">'+cells+'</span></div>'; }).join("");
    return '<div class="small muted">overall EWMA '+base.toFixed(2)+' · last 12 wk</div>'+spark+dots+'<div class="fd-card-h" style="margin-top:6px">per-action · last 8 check-ins</div>'+rows;
  }
  function forecastSvg(){
    var R=state.R, cur=R.score;
    var lift=R.nudge.slice(0,3).reduce(function(s,x){return s+x.impact*8;},0)*(0.4+0.6*R.adherence.adherence);  // adherence gates realized lift
    var pts=[cur, Math.min(100,cur+lift*0.4), Math.min(100,cur+lift*0.75), Math.min(100,cur+lift)];
    var w=210,h=46,mn=Math.min.apply(null,pts)-2,mx=Math.max.apply(null,pts)+2;
    var sx=function(i){return i/(pts.length-1)*w;}, sy=function(v){return h-((v-mn)/(mx-mn||1))*(h-8)-4;};
    var d=""; for(var i=0;i<pts.length;i++){ d+=(i?"L":"M")+sx(i).toFixed(1)+" "+sy(pts[i]).toFixed(1)+" "; }
    return '<svg width="100%" height="'+h+'" viewBox="0 0 '+w+' '+h+'" preserveAspectRatio="none">'
      +'<path d="'+d+'" fill="none" stroke="#3ad6a0" stroke-width="2"/>'
      +'<circle cx="'+sx(pts.length-1).toFixed(1)+'" cy="'+sy(pts[pts.length-1]).toFixed(1)+'" r="3" fill="#3ad6a0"/></svg>'
      +'<div class="small muted">now '+cur+' → '+Math.round(pts[pts.length-1])+' if top-3 nudges adopted (30d, illustrative)</div>';
  }

  /* ================= DESCRIPTOR TREE ================= */
  var PR=D.roots.purescore;
  function pure(){ return { id:"score", label:"PureScore", area:"score", formula:PR.formula, consts:["pure_crit_cap","delta"], doc:"03",
      json:js({formula:PR.formula,cap:D.const.pure_crit_cap}), ctx:{},
      val:function(R){ return String(R.score); }, kids:function(){ return [rtotal()]; } }; }
  function rtotal(){ var n=PR.nodes.r_total; return { id:"r_total", label:"R_total (overall risk)", area:"score", formula:n.formula, consts:["delta"], doc:"03", ctx:{},
      val:function(R){ return "R_tot = "+R.Rtot.toFixed(3); }, kids:function(){ return Object.keys(D.pillars).map(pillar); } }; }
  function pillar(pid){ var P=D.pillars[pid], n=PR.nodes.pillar;
    return { id:"p_"+pid, label:P.label, area:"pillar", formula:n.formula, consts:["rho","r_crit"], doc:P.doc, grid:"appendix-biomarkers.html#spreadsheet", ctx:{pid:pid},
      json:js({weight:P.weight,markers:P.markers,critical:P.critical,reservoirs:P.reservoirs}),
      val:function(R){ var d=R.detail[pid]; return "R="+d.R.toFixed(2)+" · "+pct(d.coverage)+(d.crit?" ⚑":"")+(d.lowCoverage?" ⚠":""); },
      kids:function(){ var k=[]; if(P.reservoirs.length) k.push(resContrib(pid)); P.markers.forEach(function(m){ k.push(marker(pid,m)); }); return k; } }; }
  function resContrib(pid){ var P=D.pillars[pid];
    return { id:"rc_"+pid, label:"+ ρ · reservoir load", area:"reservoir", formula:"ρ · mean(L_j) into "+pid.toUpperCase(), consts:["rho"], doc:"04", ctx:{pid:pid},
      val:function(R){ return "ρ·L̄ = "+(D.const.rho*R.detail[pid].bt).toFixed(3); },
      kids:function(){ return P.reservoirs.map(function(rid){ return reservoir(pid,rid); }); } }; }
  function reservoir(pid,rid){ var rr=D.reservoirs[rid];
    return { id:"res_"+pid+"_"+rid, label:rr.label, area:"reservoir", doc:"04", ctx:{pid:pid,rid:rid}, json:js(rr),
      formula:"L = clamp01(mean(nrm(inputs)) + coupling) · "+rr.polarity+" → feeds "+rr.feeds.toUpperCase(),
      val:function(R){ return "L = "+R.res.coupled[rid].toFixed(2); },
      kids:function(){ return rr.inputs.map(function(inp){ return leaf(inp[0], pid); }); } }; }
  function marker(pid,m){ var mm=D.markers[m], P=D.pillars[pid], isC=P.critical.indexOf(m)>=0;
    return { id:"m_"+pid+"_"+m, label:mm.label+(isC?" ⚑":""), area:"marker", ctx:{pid:pid,mid:m}, ismarker:true, doc:mm.doc, grid:"appendix-biomarkers.html#spreadsheet",
      formula:"r off band th=["+mm.th[0]+", "+mm.th[1]+", "+mm.th[2]+"] · channel "+mm.source+(mm.imputable?" · imputable":""),
      json:js(mm),
      val:function(R){ var md=R.detail[pid].markers[m]; return md.included?("r="+(md.r!=null?md.r.toFixed(2):"—")+" "+md.zone):"dropped"; },
      zone:function(R){ var md=R.detail[pid].markers[m]; return md.included?md.zone:"na"; },
      kids:function(){ return [pipeline(pid,m), D.instruments[m]?questionnaire(pid,m):leaf(m,pid)]; } }; }
  function pipeline(pid,m){ var stages=PR.nodes.marker.stages;
    return { id:"pl_"+pid+"_"+m, label:"pipeline — full decision path", area:"marker", doc:"03", ctx:{pid:pid,mid:m},
      formula:"confidence → band → cohort → max(r,φ·r_cohort) → personalize → critical cascade",
      val:function(){ return ""; }, kids:function(){ return stages.map(function(s){ return stageNode(pid,m,s); }); } }; }
  function stageNode(pid,m,s){
    var node={ id:"st_"+pid+"_"+m+"_"+s.id, label:s.label, area:"marker", formula:s.formula, consts:s.cRef||[], doc:s.doc, ctx:{pid:pid,mid:m},
      val:function(R){ return s.id==="band"?("r_clin = "+(R.detail[pid].markers[m].r!=null?R.detail[pid].markers[m].r.toFixed(2):"—")):""; } };
    if(s.branches){ node.kids=function(){ return s.branches.map(function(b){
      return { id:node.id+"_"+b.id, label:b.label, area:"marker", branch:true, doc:s.doc, formula:b.label, ctx:{pid:pid,mid:m},
        activeFn:function(R){ return R.detail[pid].markers[m].branch[s.id]===b.id; } }; }); }; }
    return node;
  }
  function leaf(m, pid){ var mm=D.markers[m];
    return { id:"leaf_"+pid+"_"+m, label:mm.label+" — raw input", area:"capture", leaf:m, pid:pid, ctx:{pid:pid,mid:m},
      formula:"editable raw input · "+mm.unit+" · default "+mm.default+" · channel "+mm.source+(mm.imputable?" · cohort-fallback":" · drop if missing"),
      doc:mm.doc, grid:"appendix-biomarkers.html#spreadsheet", json:js(mm),
      val:function(){ var st=state.st[m]||"present"; var used=(st==="missing")?(mm.imputable?(mm.default+" (cohort)"):"dropped"):markerVal(m); return used+" "+mm.unit+(st!=="present"?(" · "+st):""); } }; }
  function questionnaire(pid,m){ var inst=D.instruments[m];
    return { id:"q_"+pid+"_"+m, label:inst.id+" — "+inst.items.length+" items (answer to score)", area:"capture", doc:"08", ctx:{pid:pid,mid:m}, instm:m,
      formula:inst.stem+"  ·  "+inst.scale, json:js({instrument:inst.id,scoring:inst.scoring}),
      val:function(){ return "Σ = "+sumQ(m)+" / "+(inst.items.length*inst.maxlvl); },
      kids:function(){ return inst.items.map(function(txt,i){ return {qitem:true, mid:m, idx:i, maxlvl:inst.maxlvl, label:txt, area:"capture", id:"qi_"+m+"_"+i, ctx:{pid:pid,mid:m}}; }); } }; }

  /* gates + other roots */
  function gatesRoot(){ return { id:"gates", label:"Applicability gates — regardless of profile", area:"gate", ctx:{},
      formula:"Sex · pregnancy · age-band · life-stage choose cut-points, frames and eligible items before scoring.",
      val:function(R){ return "sex "+R.gates.sex+" · age "+R.gates.age+(R.gates.preg==="yes"?" · pregnant":""); },
      kids:function(){ return D.gates.map(gateNode); } }; }
  function gateNode(g){ return { id:"gate_"+g.id, label:g.label, area:"gate", formula:g.formula, doc:g.doc, json:js(g), ctx:{},
      val:function(R){ return "▶ "+(R.gates[g.id]||"—"); },
      kids:function(){ return g.branches.map(function(b){ return { id:"gate_"+g.id+"_"+b.id, label:b.label, area:"gate", branch:true, doc:g.doc, formula:g.question+" → "+b.label, ctx:{},
        activeFn:function(R){ return R.gates[g.id]===b.id; } }; }); } }; }
  function simpleRoot(key){ var r=D.roots[key], area={companion:"companion",adherence:"adherence",actuarial:"score"}[key];
    var RV={ companion:function(R){return R.companion.ew?"early-warning ON":"stable";}, adherence:function(R){return "adherence "+R.adherence.adherence.toFixed(2);}, actuarial:function(R){return "×"+R.actuarial.premium;} };
    var SV={ companion:function(nk,R){return nk==="confidence"?(R.companion.confidence.toFixed(2)+" · cov "+pct(R.companion.coverage)):nk==="trajectory"?R.companion.trajectory:(R.companion.ew?"ON":"off");},
             adherence:function(nk,R){return nk==="ewma"?R.adherence.ewma.toFixed(2):"inflow "+R.adherence.inflow;},
             actuarial:function(nk,R){return nk==="rr"?("RR "+R.actuarial.rr):("×"+R.actuarial.premium);} };
    return function(){ return { id:key, label:r.label, area:area, formula:r.formula, doc:r.doc, json:js({formula:r.formula}), ctx:{}, val:function(R){return RV[key](R);},
      kids:function(){ return Object.keys(r.nodes).map(function(nk){ var n=r.nodes[nk]; return { id:key+"_"+nk, label:n.label, area:area, formula:n.formula, consts:n.cRef||[], doc:n.doc, ctx:{}, val:function(R){return SV[key](nk,R);} }; }); } }; };
  }
  function nudgeRoot(){ var r=D.roots.nudge; return { id:"nudge", label:r.label, area:"nudge", formula:r.formula, doc:"07", json:js({formula:r.formula}), ctx:{},
      val:function(R){ return R.nudge.length+" candidates"; },
      kids:function(){ return state.R.nudge.map(function(x,i){ return { id:"nud_"+i, label:(i+1)+" · focus "+x.pillar.toUpperCase(), area:"nudge", ctx:{pid:x.pillar},
        formula:"U_a = impact("+x.impact+") · p̂("+x.phat+") · ease("+x.ease+")", doc:"07", val:function(){return "U = "+x.U;} }; }); } }; }
  var BUILDERS={ purescore:pure, companion:simpleRoot("companion"), nudge:nudgeRoot, adherence:simpleRoot("adherence"), actuarial:simpleRoot("actuarial") };

  /* ---------- render ---------- */
  var REG=[];
  function buildTree(){ REG=[]; var host=document.getElementById("ceTree"); host.innerHTML="";
    host.appendChild(renderNode(gatesRoot(),0)); host.appendChild(renderNode(BUILDERS[state.root](),0));
    var rows=host.querySelectorAll(".ce-row.has"); if(rows[0]) rows[0]._toggle(true); if(rows[1]) rows[1]._toggle(true);
    refreshValues(); var s=document.getElementById("ceSearch"); if(s&&s.value) applySearch(s.value);
  }
  function renderNode(n,depth){
    var wrap=document.createElement("div"); wrap.className="ce-node";
    wrap._stext=((n.formula||"")+" "+(n.consts||[]).join(" ")).toLowerCase();
    var row=document.createElement("div"); row.className="ce-row"; row.style.paddingLeft=(depth*15+6)+"px"; row.style.borderLeftColor=aColor(n.area); row._node=n;
    if(n.qitem){ return renderQItem(n,depth); }
    var car=document.createElement("span"); car.className="ce-car"; car.textContent=n.kids?"▸":(n.branch?"":"·"); row.appendChild(car);
    var lab=document.createElement("span"); lab.className="ce-lab"; lab.textContent=n.label; row.appendChild(lab);
    if(n.leaf){
      var mm=D.markers[n.leaf];
      var stbtn=document.createElement("button"); stbtn.className="ce-state"; stbtn.title="data-state: present → stale → missing";
      stbtn.onclick=function(e){ e.stopPropagation(); cycleState(n.leaf); }; row.appendChild(stbtn); row._stbtn=stbtn;
      var rng=document.createElement("input"); rng.type="range"; rng.className="ce-rng";
      var lo=Math.min(mm.th[0],mm.th[1],mm.default), hi=Math.max(mm.th[0],mm.th[1],mm.default), pad=(hi-lo)*0.5||Math.abs(mm.default)*0.5||1;
      rng.min=lo-pad; rng.max=hi+pad; rng.step=((rng.max-rng.min)/120)||0.1; rng.value=markerVal(n.leaf);
      rng.onclick=function(e){ e.stopPropagation(); };
      rng.oninput=function(){ state.mk[n.leaf]=parseFloat(rng.value); if(state.st[n.leaf]==="missing") delete state.st[n.leaf]; delete state.qresp[n.leaf]; recompute(); refreshAll(); };
      row.appendChild(rng); row._rng=rng; row._leaf=n.leaf;
    }
    var v=document.createElement("span"); v.className="ce-val"; row.appendChild(v); row._valEl=v;
    if(n.ismarker){ var cap=document.createElement("span"); cap.className="ce-cap"; row.appendChild(cap); row._cap=cap; row._capm=n.ctx.mid; }
    var info=document.createElement("button"); info.className="ce-i"; info.innerHTML="&#9432;"; info.title="trace"; info.onclick=function(e){ e.stopPropagation(); traceNode(n,row); }; row.appendChild(info);
    row.onmouseenter=function(ev){ showTip(ev,tipNode(n)); }; row.onmousemove=moveTip; row.onmouseleave=hideTip;
    wrap.appendChild(row); REG.push({node:n,row:row});
    if(n.kids){ var kb=document.createElement("div"); kb.className="ce-kids"; kb.style.display="none"; wrap.appendChild(kb);
      row.classList.add("has"); row._open=false; row._built=false;
      row._toggle=function(open){ if(open===undefined) open=!row._open;
        if(open&&!row._built){ n.kids().forEach(function(c){ kb.appendChild(renderNode(c,depth+1)); }); row._built=true; refreshValues(); }
        row._open=open; kb.style.display=open?"":"none"; car.textContent=open?"▾":"▸"; car.classList.toggle("open",open); };
      row.onclick=function(){ row._toggle(); traceNode(n,row); };
    } else { row.onclick=function(){ traceNode(n,row); }; }
    return wrap;
  }
  function renderQItem(n,depth){
    var wrap=document.createElement("div"); wrap.className="ce-node";
    var row=document.createElement("div"); row.className="ce-qitem"; row.style.paddingLeft=(depth*15+18)+"px";
    var q=document.createElement("span"); q.className="q"; q.textContent=n.label; row.appendChild(q);
    var opt=document.createElement("span"); opt.className="ce-qopt";
    for(var v=0; v<=n.maxlvl; v++){ (function(val){ var b=document.createElement("button"); b.textContent=val;
      b.onclick=function(e){ e.stopPropagation(); ensureQ(n.mid); state.qresp[n.mid][n.idx]=val; state.mk[n.mid]=sumQ(n.mid); recompute(); refreshAll(); };
      opt.appendChild(b); })(v); }
    row.appendChild(opt); wrap.appendChild(row); REG.push({node:n,row:row,qitem:true});
    return wrap;
  }
  function refreshValues(){
    REG.forEach(function(e){ var n=e.node, row=e.row, el=row._valEl;
      if(e.qitem){ ensureQ(n.mid); var sel=state.qresp[n.mid][n.idx]; [].forEach.call(row.querySelectorAll(".ce-qopt button"),function(b,i){ b.classList.toggle("on",i===sel); }); return; }
      if(n.branch){ var act=n.activeFn(state.R); row.classList.toggle("ce-active",act); row.classList.toggle("ce-inactive",!act); row.querySelector(".ce-car").textContent=act?"✓":"✗"; if(el) el.textContent=act?"active":""; return; }
      if(el&&n.val){ el.textContent=n.val(state.R); el.className="ce-val"+(n.zone?(" z-"+n.zone(state.R)):""); }
      if(row._leaf){ var m=row._leaf, stt=state.st[m]||"present"; row._stbtn.textContent=stt==="present"?"P":(stt==="stale"?"S":"M"); row._stbtn.className="ce-state s-"+stt; if(row._rng) row._rng.disabled=(stt==="missing"); }
      if(row._cap){ var ci=capInfo(row._capm); row._cap.textContent=ci.txt; }
    });
    var cov=document.getElementById("ceCoverage"); if(cov&&state.R) cov.textContent="coverage "+pct(state.R.coverage)+" · confidence "+state.R.confidence.toFixed(2)+(state.R.lowCoverage?" ⚠":"");
  }
  function refreshAll(){ renderBand(); renderStrip(); refreshValues(); if(state.sel) traceNode(state.sel,null,true); if(mapBuilt&&mapApi) mapApi.refresh(); }
  function expandAll(){ var g=0; while(g++<40){ var any=false; [].forEach.call(document.querySelectorAll("#ceTree .ce-row.has"),function(r){ if(!r._open){ r._toggle(true); any=true; } }); if(!any) break; } }
  function collapseAll(){ [].forEach.call(document.querySelectorAll("#ceTree .ce-row.has"),function(r){ if(r._open) r._toggle(false); }); var rows=document.querySelectorAll("#ceTree .ce-row.has"); if(rows[0]) rows[0]._toggle(true); if(rows[1]) rows[1]._toggle(true); }

  /* ---------- state ---------- */
  function cycleState(m){ var cur=state.st[m]||"present", nxt=cur==="present"?"stale":(cur==="stale"?"missing":"present"); if(nxt==="present") delete state.st[m]; else state.st[m]=nxt; recompute(); refreshAll(); syncStreamBtns(); }
  function setStream(src,missing){ for(var m in D.markers){ if(D.markers[m].source===src){ if(missing) state.st[m]="missing"; else if(state.st[m]==="missing") delete state.st[m]; } } recompute(); refreshAll(); }
  function streamMissing(src){ var any=false,all=true; for(var m in D.markers){ if(D.markers[m].source===src){ if(state.st[m]==="missing") any=true; else all=false; } } return all&&any; }
  function syncStreamBtns(){ [].forEach.call(document.querySelectorAll(".ce-strm"),function(b){ b.classList.toggle("on",streamMissing(b.getAttribute("data-src"))); }); }

  /* ---------- tooltip (hover → companion) ---------- */
  var tip=document.getElementById("ceTip");
  function showTip(ev,html){ if(!tip) return; tip.innerHTML=html; tip.style.display="block"; moveTip(ev); }
  function moveTip(ev){ if(!tip) return; var x=ev.clientX+14, y=ev.clientY+14; if(x>window.innerWidth-260) x=ev.clientX-260; tip.style.left=x+"px"; tip.style.top=y+"px"; }
  function hideTip(){ if(tip) tip.style.display="none"; }
  function tipNode(n){ var R=state.R, ctx=n.ctx||{};
    if(ctx.mid){ var md=R.detail[ctx.pid].markers[ctx.mid]; return "<b>"+esc(D.markers[ctx.mid].label)+"</b><br>r="+(md.r!=null?md.r.toFixed(2):"—")+" · state "+md.state+" · conf "+md.conf.toFixed(2)+" · w "+md.w+"<br>contribution w·r="+(md.w*(md.r||0)).toFixed(3); }
    if(ctx.pid){ return tipPillar(ctx.pid); }
    return "Confidence <b>"+R.confidence.toFixed(2)+"</b> · Coverage <b>"+pct(R.coverage)+"</b><br>Trajectory "+R.companion.trajectory+" · EW "+(R.companion.ew?"ON":"off"); }
  function tipPillar(pid){ var d=state.R.detail[pid]; return "<b>"+pid.toUpperCase()+"</b> R="+d.R.toFixed(2)+"<br>coverage "+pct(d.coverage)+" · confidence "+d.confidence.toFixed(2)+"<br>"+(d.crit?"⚑ critical ":"")+(d.lowCoverage?"⚠ low coverage":"")||"ok"; }
  function tipRes(rid){ var rr=D.reservoirs[rid]; return "<b>"+esc(rr.label)+"</b><br>load L="+state.R.res.coupled[rid].toFixed(2)+"<br>"+rr.polarity+" · feeds "+rr.feeds.toUpperCase(); }

  /* ---------- execution trace ---------- */
  function selectRow(row){ [].forEach.call(document.querySelectorAll("#ceTree .ce-row.sel"),function(r){ r.classList.remove("sel"); }); if(row) row.classList.add("sel"); }
  function traceCtx(ctx,label){ traceNode({label:label,ctx:ctx,formula:(ctx.rid?D.reservoirs[ctx.rid]?("L feeds "+D.reservoirs[ctx.rid].feeds.toUpperCase()):"":(ctx.pid?"R_k = clamp01(mean(r)+ρ·load)":"")) },null); }
  function traceNode(n,row,keep){ state.sel=n; if(!keep) selectRow(row);
    var R=state.R, b=document.getElementById("ceTraceBody"), p=[], ctx=n.ctx||{};
    p.push('<b>'+esc(n.label)+'</b>');
    if(n.formula) p.push('<div class="fd-k">formula / condition</div><pre>'+esc(n.formula)+'</pre>');
    var path=[]; function li(l,v){ return '<li>'+l+' <b>'+esc(v)+'</b></li>'; }
    path.push(li("PureScore","= "+R.score)); path.push(li("R_total","= "+R.Rtot.toFixed(3)+" (δ-power-mean)"));
    if(ctx.pid){ var d=R.detail[ctx.pid]; path.push(li(ctx.pid.toUpperCase()+" pillar","R="+d.R.toFixed(2)+" · W "+d.weight+" · cov "+pct(d.coverage)+" · conf "+d.confidence.toFixed(2)+(d.crit?" ⚑":"")));
      if(ctx.rid){ path.push(li("reservoir "+ctx.rid.toUpperCase(),"L="+R.res.coupled[ctx.rid].toFixed(2)+" → ρ·L̄ "+(D.const.rho*d.bt).toFixed(3))); }
      if(ctx.mid){ var md=d.markers[ctx.mid]; path.push(li("marker "+ctx.mid,"r="+(md.r!=null?md.r.toFixed(2):"—")+" · "+md.state+(md.imputed?" imputed":"")+" · w "+md.w+" · conf "+md.conf.toFixed(2)));
        path.push(li("→ contribution","w·r = "+(md.w*(md.r||0)).toFixed(3)+" into pillar mean")); } }
    p.push('<div class="fd-k">execution path (top → here)</div><ul class="fd-path">'+path.join("")+'</ul>');
    var lv=n.val?n.val(R):""; if(lv) p.push('<div class="fd-live">live: <b>'+esc(lv)+'</b> &middot; <span class="small muted">'+esc(curProfile().name)+'</span></div>');
    if(ctx.pid && !ctx.mid && !ctx.rid) p.push(pillarTrendHtml(ctx.pid));
    if(ctx.mid) p.push(markerTrace(ctx.mid,ctx.pid));
    if(n.consts&&n.consts.length) p.push('<div class="fd-k">constants</div><div class="ce-chips">'+n.consts.map(function(c){ return '<a class="ce-chip" href="03-scoring-formula.html">'+c+" = "+(D.const[c]!=null?D.const[c]:"?")+'</a>'; }).join(" ")+'</div>');
    if(n.json) p.push('<div class="fd-k">source JSON · data/calc-graph.json</div><pre>'+esc(n.json)+'</pre>');
    var links=[]; if(n.doc) links.push('<a class="ce-chip" href="'+docHref(n.doc)+'">'+docLabel(n.doc)+'</a>'); if(n.grid) links.push('<a class="ce-chip" href="'+n.grid+'">grid ↗</a>');
    if(links.length) p.push('<div class="fd-k">links</div><div class="ce-chips">'+links.join(" ")+'</div>');
    b.innerHTML=p.join("");
  }
  function markerTrace(mid,pid){ var md=state.R.detail[pid].markers[mid], mm=D.markers[mid], p=[];
    p.push('<div class="fd-k">values & weights</div><ul class="fd-path"><li>value <b>'+esc(String(md.v))+(mm.unit?(" "+mm.unit):"")+'</b></li>'
      +'<li>risk r <b>'+(md.r!=null?md.r.toFixed(2):"—")+'</b> ('+md.zone+')</li>'
      +'<li>state <b>'+md.state+(md.imputed?" · imputed":"")+'</b> · confidence <b>'+md.conf.toFixed(2)+'</b> · weight <b>'+md.w+'</b></li>'
      +'<li>contribution to pillar <b>'+(md.w*(md.r||0)).toFixed(3)+'</b></li></ul>');
    if(md.managed||md.confounded) p.push('<div class="fd-k">medication effect (D3)</div><div class="ce-chips">'+(md.managed?'<span class="ce-chip">MANAGED · shown but tagged</span>':"")+(md.confounded?'<span class="ce-chip">confounded · confidence↓</span>':"")+'</div>');
    p.push('<div class="fd-k">branches taken</div><ul class="fd-path">'
      +"<li>confidence/fallback: <b>"+md.branch.conf+"</b></li><li>cohort blend: <b>"+md.branch.blend+"</b></li>"
      +"<li>personalization: <b>"+md.branch.pers+"</b></li><li>critical cascade: <b>"+md.branch.crit+"</b></li></ul>");
    var ci=capInfo(mid); p.push('<div class="fd-k">provenance</div><div class="ce-chips"><span class="ce-chip">channel '+mm.source+'</span><span class="ce-chip">'+esc(ci.txt)+'</span>'+(mm.imputable?'<span class="ce-chip">imputable</span>':'<span class="ce-chip">non-imputable</span>')+'</div>');
    var wi=wearInfo(mid); if(wi){ p.push('<div class="fd-k">wearable trust (D22)</div><div class="ce-chips"><span class="ce-chip">tier '+esc(wi.tier)+'</span><span class="ce-chip">±'+esc(wi.acc)+'</span>'+(wi.dev?'<span class="ce-chip">'+esc(wi.dev)+'</span>':'')+'</div>');
      if((state.st[mid]||"present")!=="missing") p.push(wbDetailHtml(mid)); }
    if(D.instruments[mid]){ var inst=D.instruments[mid]; ensureQ(mid); p.push('<div class="fd-k">'+inst.id+' · '+sumQ(mid)+' / '+(inst.items.length*inst.maxlvl)+'</div><div class="small muted">'+esc(inst.scoring||inst.stem)+'</div>'); }
    return p.join("");
  }

  /* ---------- search ---------- */
  function parentNode_(nd){ var k=nd.parentNode; return (k&&k.classList&&k.classList.contains("ce-kids"))?k.parentNode:null; }
  function applySearch(q){ var host=document.getElementById("ceTree"); q=(q||"").trim().toLowerCase();
    if(!q){ [].forEach.call(host.querySelectorAll(".ce-node"),function(nd){ nd.style.display=""; }); collapseAll(); return; }
    expandAll(); var list=[].slice.call(host.querySelectorAll(".ce-node"));
    list.forEach(function(nd){ var row=nd.querySelector(".ce-row")||nd.querySelector(".ce-qitem"); var t=((row?row.textContent:"")+" "+(nd._stext||"")).toLowerCase(); nd._ms=t.indexOf(q)>=0; nd._md=false; nd._ma=false; });
    for(var i=list.length-1;i>=0;i--){ var nd=list[i],pp=parentNode_(nd); if(pp&&(nd._ms||nd._md)) pp._md=true; }
    list.forEach(function(nd){ var pp=parentNode_(nd); if(pp&&(pp._ms||pp._ma)) nd._ma=true; });
    list.forEach(function(nd){ nd.style.display=(nd._ms||nd._md||nd._ma)?"":"none"; });
  }

  /* ---------- map (engine-backed flow view) ---------- */
  var mapBuilt=false, mapApi=null;
  var BASE=[{id:"onb",a:"onboard",l:"Onboarding",c:0,r:6,k:"process"},{id:"lab",a:"capture",l:"Labs",c:1,r:4,k:"store"},{id:"wear",a:"capture",l:"Wearables",c:1,r:5,k:"store"},{id:"ehr",a:"capture",l:"EHR",c:1,r:6,k:"store"},{id:"life",a:"capture",l:"PRO / lifestyle",c:1,r:7,k:"store"},{id:"gate",a:"gate",l:"Eligibility & gating",c:2,r:5.5,k:"gate"},{id:"conf",a:"marker",l:"Confidence + decay",c:3,r:3.5,k:"gate"},{id:"band",a:"marker",l:"Clinical band",c:3,r:4.5,k:"process"},{id:"cohort",a:"marker",l:"Cohort percentile",c:3,r:5.5,k:"process"},{id:"mmax",a:"marker",l:"max(r,φ·r_cohort)",c:3,r:6.5,k:"gate"},{id:"pers",a:"marker",l:"Personalize",c:3,r:7.5,k:"gate"},{id:"agg",a:"score",l:"δ-power-mean",c:6,r:5,k:"process"},{id:"crit",a:"score",l:"Critical cascade",c:6,r:7,k:"gate"},{id:"score",a:"score",l:"PureScore",c:7,r:5,k:"output"},{id:"esc",a:"safety",l:"Escalation",c:7,r:8,k:"gate"},{id:"comp",a:"companion",l:"Companion",c:8,r:3.5,k:"output"},{id:"nud",a:"nudge",l:"Nudges",c:8,r:5.5,k:"process"},{id:"adh",a:"adherence",l:"Adherence",c:9,r:5.5,k:"process"},{id:"recal",a:"recal",l:"Recalibration",c:10,r:5.5,k:"process"}];
  function mapVal(id){ var R=state.R; if(!R) return "";
    if(id==="score") return String(R.score); if(D.pillars[id]) return "R="+R.rk[id].toFixed(2)+(R.crit.indexOf(id)>=0?" ⚑":"");
    if(D.reservoirs[id]) return "L="+R.res.coupled[id].toFixed(2); if(id==="agg") return "R_tot="+R.Rtot.toFixed(2);
    if(id==="crit") return R.crit.length?("crit "+R.crit.join(",")):"none"; if(id==="comp") return R.companion.ew?"EW ON":"stable";
    if(id==="adh") return "adher "+R.adherence.adherence.toFixed(2); if(id==="conf") return "conf "+R.confidence.toFixed(2); return ""; }
  function buildMap(){ if(mapBuilt) return; mapBuilt=true; var NS="http://www.w3.org/2000/svg";
    function el(t,at){ var e=document.createElementNS(NS,t); for(var k in at) e.setAttribute(k,at[k]); return e; }
    var N=BASE.map(function(b){return {id:b.id,a:b.a,l:b.l,c:b.c,r:b.r,k:b.k};});
    Object.keys(D.reservoirs).forEach(function(rid,i){ N.push({id:rid,a:"reservoir",l:D.reservoirs[rid].label,c:4,r:i,k:"store"}); });
    Object.keys(D.pillars).forEach(function(pid,i){ N.push({id:pid,a:"pillar",l:D.pillars[pid].label,c:5,r:i,k:"pillar"}); });
    var E=[["onb","gate"],["lab","gate"],["wear","gate"],["ehr","gate"],["life","gate"],["gate","conf"],["conf","band"],["conf","cohort"],["band","mmax"],["cohort","mmax"],["mmax","pers"],["agg","crit"],["crit","score"],["crit","esc"],["score","comp"],["score","nud"],["nud","adh"],["adh","recal"],["recal","score",1]];
    Object.keys(D.pillars).forEach(function(pid){ E.push(["pers",pid]); E.push([pid,"agg"]); });
    Object.keys(D.reservoirs).forEach(function(rid){ E.push([rid,D.reservoirs[rid].feeds]); });
    var COLW=210,ROWH=64,PADX=110,PADY=54,NW=170,NH=44,byId={};
    N.forEach(function(n){ n.x=PADX+n.c*COLW; n.y=PADY+n.r*ROWH; byId[n.id]=n; });
    var maxRow=0,maxCol=0; N.forEach(function(n){ maxRow=Math.max(maxRow,n.r); maxCol=Math.max(maxCol,n.c); });
    var W=PADX*2+maxCol*COLW+NW,H=PADY*2+maxRow*ROWH+NH;
    var svg=document.getElementById("umSvg"),wrap=document.getElementById("umWrap"),scene=el("g",{}); svg.appendChild(scene);
    var areaG=el("g",{}); scene.appendChild(areaG); var groups={}; N.forEach(function(n){ (groups[n.a]=groups[n.a]||[]).push(n); });
    D.areas.forEach(function(a){ var ns=groups[a.k]; if(!ns) return; var x0=1e9,y0=1e9,x1=-1e9,y1=-1e9; ns.forEach(function(n){ x0=Math.min(x0,n.x);y0=Math.min(y0,n.y);x1=Math.max(x1,n.x+NW);y1=Math.max(y1,n.y+NH); });
      var pd=14; areaG.appendChild(el("rect",{x:x0-pd,y:y0-pd-15,width:(x1-x0)+pd*2,height:(y1-y0)+pd*2+15,rx:13,fill:a.c,"fill-opacity":0.06,stroke:a.c,"stroke-opacity":0.3,"class":"um-areabg","data-area":a.k}));
      var tx=el("text",{x:x0-pd+6,y:y0-pd-3,fill:a.c,"class":"um-arealabel","data-area":a.k}); tx.textContent=a.l; areaG.appendChild(tx); });
    var edgeG=el("g",{}); scene.appendChild(edgeG); E.forEach(function(e){ var s=byId[e[0]],t=byId[e[1]]; if(!s||!t) return; var x1=s.x+NW,y1=s.y+NH/2,x2=t.x,y2=t.y+NH/2; if(t.x<=s.x){x1=s.x+NW/2;y1=s.y+NH;x2=t.x+NW/2;y2=t.y;} var mx=(x1+x2)/2; edgeG.appendChild(el("path",{d:"M"+x1+","+y1+" C"+mx+","+y1+" "+mx+","+y2+" "+x2+","+y2,"class":"um-edge"+(e[2]?" loop":"")})); });
    var nodeG=el("g",{}); scene.appendChild(nodeG); var nodeEls={};
    N.forEach(function(n){ var g=el("g",{"class":"um-node","data-id":n.id}); var fill=n.k==="gate"?"#1d1a0c":(n.k==="output"?"#0c1726":(n.k==="store"?"#10151e":(n.k==="pillar"?"#10211a":"#121a26")));
      g.appendChild(el("rect",{x:n.x,y:n.y,width:NW,height:NH,rx:n.k==="gate"?12:8,fill:fill,stroke:aColor(n.a),"stroke-width":1.5}));
      var t1=el("text",{x:n.x+10,y:n.y+18}); t1.textContent=n.l; g.appendChild(t1); var tv=el("text",{x:n.x+10,y:n.y+34,"class":"um-val","data-val":n.id}); g.appendChild(tv);
      g.addEventListener("click",function(ev){ ev.stopPropagation(); if(!moved) traceCtx(D.pillars[n.id]?{pid:n.id}:(D.reservoirs[n.id]?{pid:D.reservoirs[n.id].feeds,rid:n.id}:{}), n.l); });
      nodeG.appendChild(g); nodeEls[n.id]=g; });
    var tx=0,ty=0,k=1,down=false,moved=false,lx=0,ly=0;
    function apply(){ scene.setAttribute("transform","translate("+tx+","+ty+") scale("+k+")"); }
    function fit(){ var bb=wrap.getBoundingClientRect(); k=Math.max(0.18,Math.min(1.3,Math.min(bb.width/W,bb.height/H)*0.96)); tx=(bb.width-W*k)/2; ty=(bb.height-H*k)/2; apply(); }
    svg.addEventListener("wheel",function(ev){ ev.preventDefault(); var bb=svg.getBoundingClientRect(),mx=ev.clientX-bb.left,my=ev.clientY-bb.top,f=ev.deltaY<0?1.12:1/1.12,nk=Math.max(0.15,Math.min(3,k*f)); tx=mx-(mx-tx)*(nk/k); ty=my-(my-ty)*(nk/k); k=nk; apply(); },{passive:false});
    svg.addEventListener("pointerdown",function(ev){ down=true; moved=false; lx=ev.clientX; ly=ev.clientY; });
    svg.addEventListener("pointermove",function(ev){ if(!down) return; var dx=ev.clientX-lx,dy=ev.clientY-ly; if(!moved&&Math.abs(dx)+Math.abs(dy)>4){ moved=true; svg.classList.add("drag"); } if(moved){ tx+=dx; ty+=dy; lx=ev.clientX; ly=ev.clientY; apply(); } });
    window.addEventListener("pointerup",function(){ down=false; svg.classList.remove("drag"); });
    var hidden={},ab=document.getElementById("umAreas"); ab.innerHTML="";
    D.areas.forEach(function(a){ var s=document.createElement("span"); s.textContent=a.l; s.style.color=a.c; s.style.borderColor=a.c; s.onclick=function(){ hidden[a.k]=!hidden[a.k]; s.classList.toggle("off",hidden[a.k]); flt(); }; ab.appendChild(s); });
    function flt(){ var go=document.getElementById("umGates").checked; N.forEach(function(n){ nodeEls[n.id].style.display=(hidden[n.a]||(go&&n.k!=="gate"))?"none":""; }); [].forEach.call(areaG.querySelectorAll("[data-area]"),function(e){ e.style.display=hidden[e.getAttribute("data-area")]?"none":""; }); }
    function refresh(){ N.forEach(function(n){ nodeEls[n.id].querySelector("[data-val]").textContent=mapVal(n.id); nodeEls[n.id].classList.add("on"); }); }
    document.getElementById("umGates").onchange=flt; document.getElementById("umFit").onclick=fit;
    document.getElementById("umFs").onclick=function(){ wrap.classList.toggle("fs"); setTimeout(fit,60); };
    document.addEventListener("keydown",function(e){ if(e.key==="Escape"&&wrap.classList.contains("fs")){ wrap.classList.remove("fs"); setTimeout(fit,60); } });
    flt(); fit(); refresh(); mapApi={fit:fit,refresh:refresh};
  }

  /* ---------- profile + view + init ---------- */
  function seedStateFor(p){ var mk=Object.assign({},p.mk), st={};
    if(p.have && p.have!=="all"){ var hs={}; p.have.forEach(function(m){hs[m]=1;}); (p.stale||[]).forEach(function(m){hs[m]=1;}); for(var k in p.mk) hs[k]=1;
      for(var m in D.markers){ if(!hs[m]) st[m]="missing"; } }
    (p.stale||[]).forEach(function(m){ st[m]="stale"; });
    return {mk:mk, st:st}; }
  function seedState(){ var p=curProfile(), s=seedStateFor(p); state.mk=s.mk; state.st=s.st; state.qresp={}; state.dx=(p.dx||[]).slice(); state.meds=(p.meds||[]).slice(); state.adhBoost=0; state.appliedAdh={}; }
  function computeBaseline(){ var p=curProfile(), b=seedStateFor(p); state.baseMk=b.mk; state.baseSt=b.st;
    state.baseR=PS.score(b.mk,{adh:p.adh,preg:p.preg,sex:p.sex,age:p.age,lifestage:p.lifestage,st:b.st}); }
  function loadProfile(i){ state.prof=i; seedState(); computeBaseline(); state.sel=null; recompute(); buildTree(); renderBand(); renderStrip(); syncStreamBtns();
    document.getElementById("ceTraceBody").innerHTML='<p class="muted small">Click any node (tree, pillar dial, or reservoir tank) to trace its complete execution path.</p>'; }
  function setView(v){ state.view=v;
    [].forEach.call(document.querySelectorAll(".ce-tab"),function(t){ t.classList.toggle("active",t.getAttribute("data-view")===v); });
    document.getElementById("ceCockpit").style.display=v==="tree"?"":"none";
    [].forEach.call(document.querySelectorAll(".ce-maponly"),function(e){ e.style.display=v==="map"?(e.id==="umAreas"?"flex":(e.id==="umWrap"?"block":"")):"none"; });
    if(v==="map"){ buildMap(); if(mapApi){ mapApi.refresh(); setTimeout(mapApi.fit,40); } }
  }
  function init(){
    var sel=document.getElementById("ceProfile"); D.profiles.forEach(function(p,i){ var o=document.createElement("option"); o.value=i; o.textContent=p.name; sel.appendChild(o); }); sel.onchange=function(){ loadProfile(+sel.value); };
    var rsel=document.getElementById("ceRoot"); [["purescore","PureScore"],["companion","Companion vector"],["nudge","Nudge utility U_a"],["adherence","Adherence → inflow"],["actuarial","Actuarial premium"]].forEach(function(r){ var o=document.createElement("option"); o.value=r[0]; o.textContent=r[1]; rsel.appendChild(o); }); rsel.onchange=function(){ state.root=rsel.value; buildTree(); };
    document.getElementById("ceExpand").onclick=expandAll; document.getElementById("ceCollapse").onclick=collapseAll;
    document.getElementById("ceResetMk").onclick=function(){ seedState(); recompute(); buildTree(); renderBand(); renderStrip(); syncStreamBtns(); };
    var srch=document.getElementById("ceSearch"); if(srch) srch.oninput=function(){ applySearch(srch.value); };
    [].forEach.call(document.querySelectorAll(".ce-strm"),function(b){ b.onclick=function(){ var s=b.getAttribute("data-src"); setStream(s,!streamMissing(s)); syncStreamBtns(); }; });
    [].forEach.call(document.querySelectorAll(".ce-tab"),function(t){ t.onclick=function(){ setView(t.getAttribute("data-view")); }; });
    var fb=document.getElementById("ceFocus"); if(fb) fb.onclick=function(){ document.body.classList.toggle("cefocus"); var on=document.body.classList.contains("cefocus"); fb.innerHTML=on?"⤡ exit focus":"⤢ focus"; setTimeout(onScroll,60); };
    window.addEventListener("scroll",onScroll,{passive:true});
    var simT=document.getElementById("fdSimType"); if(simT) simT.onchange=populateSim;
    var simA=document.getElementById("fdSimAdd"); if(simA) simA.onclick=simAdd;
    renderBandShell(); snapConfig(); renderConfig(); loadProfile(0); populateSim(); setView("tree");
  }
  function onScroll(){ var band=document.querySelector(".fd-band"); if(!band) return; var y=(typeof window.scrollY==="number")?window.scrollY:0; band.classList.toggle("cond", y>200 && state.view==="tree"); }
  if(document.readyState==="loading") document.addEventListener("DOMContentLoaded",init); else init();
})();
