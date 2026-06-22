#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Author data/reservoir-flows.json — the canonical 15-reservoir MONIAC flow model
(the runnable companion to Doc 04). Codes/polarity/feeds are anchored to
calc-graph.json's `reservoirs`; the 5 canonical reservoir_coupling edges are preserved.
Ported from the BIOMONIAC sim's proven 9-reservoir cardiometabolic core and EXPANDED to
the full 15 (adds hepf + the 7 assets crf/mus/bon/renr/micr/oxd/iron).

Flow tuple: {src, tgt, s(+1/-1), g(gain 0-100 scale), sh(shape), t(tau days), c(confidence 0-1)}
shapes: linear | saturating | log | threshold | j_curve   (same transfer-function library as the sim)
New / less-established edges carry lower confidence and an explicit basis tag.
Run:  python3 data/_gen_reservoir_flows.py    (from docs/tech/)
"""
import json, os

HERE = os.path.dirname(os.path.abspath(__file__))

# ---- 15 canonical reservoirs (codes/polarity/feeds from calc-graph.json) ----
# sp=setpoint, tau=relaxation days, sat=[lo,hi], bio=marker readout (level -> human marker)
RES = [
 # burdens
 {"id":"gly","name":"Glycemic","pol":"burden","feeds":"met","sp":30,"tau":21,"sat":[0,100],"bio":"HbA1c {p1} %|4.8|0.045"},
 {"id":"ath","name":"Atherogenic","pol":"burden","feeds":"cv","sp":40,"tau":30,"sat":[5,100],"bio":"ApoB {p0} mg/dL|50|1.1"},
 {"id":"vbp","name":"Vascular / BP","pol":"burden","feeds":"cv","sp":35,"tau":14,"sat":[5,100],"bio":"SBP {p0} mmHg|100|0.7"},
 {"id":"adi","name":"Adiposity","pol":"burden","feeds":"met","sp":35,"tau":60,"sat":[2,100],"bio":"waist {p0} cm|70|0.45"},
 {"id":"hepf","name":"Hepatic fat","pol":"burden","feeds":"hep","sp":25,"tau":45,"sat":[0,100],"bio":"FIB-4 {p1}|0.5|0.022"},
 {"id":"infl","name":"Inflammation","pol":"burden","feeds":"inf","sp":30,"tau":21,"sat":[2,100],"bio":"hs-CRP {p1} mg/L|0.0|0.0","bioexp":"crp"},
 {"id":"allo","name":"Allostatic load","pol":"burden","feeds":"mcs","sp":25,"tau":14,"sat":[0,100],"bio":"load {p0}/100|0|1"},
 {"id":"sld","name":"Sleep debt","pol":"burden","feeds":"slp","sp":15,"tau":5,"sat":[0,100],"bio":"deficit {p1} h/ngt|0|0.09"},
 # assets
 {"id":"crf","name":"Cardiorespiratory","pol":"asset","feeds":"fit","sp":50,"tau":42,"sat":[5,98],"bio":"VO2max {p0} mL/kg/min|20|0.4"},
 {"id":"mus","name":"Muscle reserve","pol":"asset","feeds":"bcm","sp":50,"tau":90,"sat":[5,98],"bio":"ALMI {p1} kg/m2|5.5|0.025"},
 {"id":"bon","name":"Bone reserve","pol":"asset","feeds":"bcm","sp":55,"tau":365,"sat":[5,98],"bio":"T-score {p1}|-2.5|0.035"},
 {"id":"renr","name":"Renal reserve","pol":"asset","feeds":"ren","sp":60,"tau":180,"sat":[5,99],"bio":"eGFR {p0} mL/min|45|0.6"},
 {"id":"micr","name":"Micronutrient","pol":"asset","feeds":"nut","sp":55,"tau":30,"sat":[5,98],"bio":"vit-D {p0} nmol/L|25|0.85"},
 {"id":"oxd","name":"Oxygen delivery","pol":"asset","feeds":"hem","sp":55,"tau":30,"sat":[10,99],"bio":"SpO2 {p0} %|92|0.08"},
 {"id":"iron","name":"Iron reserve","pol":"asset","feeds":"hem","sp":55,"tau":45,"sat":[5,98],"bio":"ferritin {p0} ug/L|15|1.1"},
]

# ---- valves: behavioral + Rx (sim's 16 + 3 new for the asset reservoirs) ----
VALVES = [
 {"id":"V_STEPS","name":"Daily Steps","rng":[1000,15000],"unit":"steps/day","up":True,"p":0},
 {"id":"V_MVPA","name":"Aerobic Exercise","rng":[0,420],"unit":"min/wk","up":True,"p":0},
 {"id":"V_RESIST","name":"Resistance Training","rng":[0,5],"unit":"sess/wk","up":True,"p":1},
 {"id":"V_SLP_DUR","name":"Sleep Duration","rng":[4,9],"unit":"h/night","up":True,"p":1},
 {"id":"V_SLP_REG","name":"Sleep Regularity","rng":[30,95],"unit":"SRI pts","up":True,"p":0},
 {"id":"V_DIET_PATTERN","name":"Diet Quality (MEDAS)","rng":[0,14],"unit":"pts","up":True,"p":1},
 {"id":"V_PROTEIN","name":"Protein Intake","rng":[0.4,2.0],"unit":"g/kg/day","up":True,"p":1},
 {"id":"V_MINDFULNESS","name":"Mindfulness","rng":[0,60],"unit":"min/day","up":True,"p":0},
 {"id":"V_LIGHT","name":"Light Hygiene","rng":[0,100],"unit":"pts","up":True,"p":0},
 {"id":"V_VITD","name":"Vitamin D / Sun","rng":[0,4000],"unit":"IU/day","up":True,"p":0},
 {"id":"V_IRON","name":"Iron-rich Diet","rng":[0,100],"unit":"pts","up":True,"p":0},
 {"id":"V_ENERGY_BAL","name":"Energy Balance","rng":[-750,750],"unit":"kcal/day","up":False,"p":0},
 {"id":"V_GLYCEMIC_LOAD","name":"Glycemic Load","rng":[40,250],"unit":"GL/day","up":False,"p":0},
 {"id":"V_SODIUM","name":"Sodium Intake","rng":[1000,6000],"unit":"mg/day","up":False,"p":0},
 {"id":"V_ALCOHOL","name":"Alcohol","rng":[0,35],"unit":"drinks/wk","up":False,"p":1},
 {"id":"V_NICOTINE","name":"Tobacco","rng":[0,40],"unit":"cig/day","up":False,"p":0},
]
RX_VALVES = [
 {"id":"V_MED_STATIN","name":"Statin","rng":[0,3],"unit":"intensity","up":True,"p":1,"rx":True},
 {"id":"V_MED_ANTIHTN","name":"Antihypertensive","rng":[0,3],"unit":"intensity","up":True,"p":1,"rx":True},
 {"id":"V_MED_GLP1","name":"GLP-1 RA","rng":[0,3],"unit":"intensity","up":True,"p":1,"rx":True},
]

OUT = [
 {"id":"O_ASCVD","name":"10-yr ASCVD Risk","pol":"burden","w":.25},
 {"id":"O_T2DM","name":"Type 2 Diabetes","pol":"burden","w":.15},
 {"id":"O_MORT","name":"Mortality Hazard","pol":"burden","w":.30},
 {"id":"O_ENERGY","name":"Vitality & Energy","pol":"asset","w":.20},
 {"id":"O_BIOAGE","name":"Biological Age Gap","pol":"burden","w":.10},
]

def F(src,tgt,s,g,sh,t,c,basis=""):
    d={"src":src,"tgt":tgt,"s":s,"g":g,"sh":sh,"t":t,"c":round(c,2)}
    if basis: d["basis"]=basis
    return d

FLOWS = [
 # ---------- valve -> reservoir (sim core, remapped to canonical ids) ----------
 F("V_MVPA","crf",1,28,"saturating",42,.92), F("V_MVPA","gly",-1,18,"saturating",7,.9),
 F("V_MVPA","adi",-1,10,"saturating",60,.8), F("V_MVPA","vbp",-1,9,"saturating",21,.85),
 F("V_MVPA","infl",-1,8,"saturating",30,.75), F("V_MVPA","allo",-1,10,"saturating",28,.6,"exercise lowers allostatic load (recovery; folds sim R_ANS)"),
 F("V_MVPA","bon",1,5,"saturating",180,.5,"weight-bearing aerobic -> bone (new)"),
 F("V_STEPS","gly",-1,9,"saturating",10,.8), F("V_STEPS","adi",-1,6,"saturating",60,.7),
 F("V_RESIST","gly",-1,8,"saturating",21,.75), F("V_RESIST","adi",-1,5,"saturating",60,.65),
 F("V_RESIST","crf",1,7,"saturating",42,.6),
 F("V_RESIST","mus",1,22,"saturating",60,.85,"resistance training -> muscle reserve (new)"),
 F("V_RESIST","bon",1,12,"saturating",180,.7,"resistance/load -> bone reserve (new)"),
 F("V_PROTEIN","mus",1,16,"saturating",60,.7,"adequate protein -> muscle reserve (new)"),
 F("V_SLP_DUR","sld",-1,30,"linear",3,.9), F("V_SLP_DUR","gly",-1,12,"threshold",5,.85),
 F("V_SLP_DUR","allo",-1,14,"saturating",6,.75,"sleep -> recovery (folds sim R_ANS + R_STR)"),
 F("V_SLP_REG","sld",-1,12,"linear",7,.7), F("V_SLP_REG","allo",-1,8,"saturating",14,.6),
 F("V_SLP_REG","gly",-1,7,"saturating",14,.65),
 F("V_ENERGY_BAL","adi",1,22,"linear",45,.9),
 F("V_GLYCEMIC_LOAD","gly",1,16,"saturating",14,.8), F("V_GLYCEMIC_LOAD","ath",1,7,"saturating",30,.6),
 F("V_SODIUM","vbp",1,14,"saturating",10,.85),
 F("V_DIET_PATTERN","ath",-1,12,"saturating",30,.8), F("V_DIET_PATTERN","infl",-1,12,"saturating",30,.75),
 F("V_DIET_PATTERN","vbp",-1,8,"saturating",21,.8),
 F("V_DIET_PATTERN","micr",1,10,"saturating",30,.6,"diet quality -> micronutrient status (new)"),
 F("V_DIET_PATTERN","iron",1,5,"saturating",45,.5,"diet quality -> iron status (new)"),
 F("V_ALCOHOL","vbp",1,9,"saturating",10,.8), F("V_ALCOHOL","sld",1,8,"saturating",2,.75),
 F("V_ALCOHOL","ath",1,5,"saturating",21,.55), F("V_ALCOHOL","infl",1,5,"j_curve",21,.5),
 F("V_ALCOHOL","hepf",1,10,"saturating",30,.7,"alcohol -> hepatic fat (new)"),
 F("V_NICOTINE","ath",1,6,"saturating",30,.6), F("V_NICOTINE","infl",1,10,"saturating",30,.75),
 F("V_MINDFULNESS","allo",-1,12,"saturating",21,.65),
 F("V_LIGHT","sld",-1,8,"saturating",10,.6), F("V_LIGHT","allo",-1,4,"saturating",21,.45),
 F("V_VITD","bon",1,12,"saturating",90,.7,"vitamin D -> bone reserve (new)"),
 F("V_VITD","micr",1,15,"saturating",30,.7,"vitamin D -> micronutrient status (new)"),
 F("V_VITD","infl",-1,4,"saturating",30,.4,"vitamin D anti-inflammatory (low conf, new)"),
 F("V_IRON","iron",1,18,"saturating",45,.7,"iron-rich diet -> iron reserve (new)"),
 F("V_IRON","oxd",1,6,"saturating",30,.5,"iron -> oxygen delivery (new)"),
 # Rx
 F("V_MED_STATIN","ath",-1,45,"log",21,.98), F("V_MED_ANTIHTN","vbp",-1,40,"log",14,.97),
 F("V_MED_GLP1","gly",-1,35,"log",30,.95), F("V_MED_GLP1","adi",-1,30,"log",90,.95),
 # ---------- reservoir -> reservoir (sim coupling + canonical kappa + new) ----------
 F("adi","gly",1,20,"saturating",30,.85), F("adi","vbp",1,12,"saturating",30,.75),
 F("adi","infl",1,10,"saturating",30,.85,"canonical kappa infl<-adi 0.10"),
 F("adi","ath",1,12,"saturating",30,.7),
 F("adi","hepf",1,18,"saturating",30,.8,"visceral adiposity -> hepatic fat (new)"),
 F("sld","gly",1,10,"threshold",7,.8,"canonical kappa gly<-sld 0.10"),
 F("sld","infl",1,15,"saturating",7,.75,"canonical kappa infl<-sld 0.15"),
 F("sld","allo",1,12,"saturating",7,.75,"canonical kappa allo<-sld 0.12 (+ sim spiral)"),
 F("allo","sld",1,12,"saturating",5,.6,"canonical kappa sld<-allo 0.12 (vicious spiral)"),
 F("allo","vbp",1,9,"saturating",14,.6), F("allo","gly",1,8,"saturating",14,.55),
 F("gly","ath",1,15,"saturating",60,.7,"canonical kappa ath<-gly 0.15"),
 F("infl","ath",1,15,"saturating",60,.7,"canonical kappa ath<-infl 0.15"),
 F("crf","gly",-1,8,"saturating",30,.6,"canonical kappa gly<-crf deficit 0.08"),
 F("crf","allo",-1,10,"saturating",30,.6), F("crf","infl",-1,8,"saturating",30,.6),
 F("hepf","gly",1,10,"saturating",45,.6,"fatty liver -> insulin resistance (new)"),
 F("hepf","infl",1,8,"saturating",45,.55,"hepatic fat -> inflammation (new)"),
 F("hepf","ath",1,8,"saturating",60,.5,"hepatic fat -> atherogenic lipids (new)"),
 F("mus","gly",-1,12,"saturating",30,.7,"muscle = glucose sink (new)"),
 F("mus","crf",1,6,"saturating",60,.5,"muscle supports fitness (new)"),
 F("vbp","renr",-1,12,"saturating",180,.7,"hypertension depletes renal reserve (new)"),
 F("gly","renr",-1,10,"saturating",180,.6,"hyperglycaemia depletes renal reserve (new)"),
 F("iron","oxd",1,15,"saturating",30,.7,"iron -> oxygen delivery (new)"),
 F("micr","iron",1,6,"saturating",45,.5,"micronutrients support iron status (new)"),
 F("micr","infl",-1,6,"saturating",30,.5,"micronutrients lower inflammation (new)"),
 F("oxd","crf",1,8,"saturating",30,.55,"oxygen delivery supports fitness (new)"),
 # ---------- reservoir/valve -> outcome ----------
 F("gly","O_ASCVD",1,14,"saturating",120,.8), F("vbp","O_ASCVD",1,22,"linear",120,.9),
 F("ath","O_ASCVD",1,22,"linear",180,.95), F("infl","O_ASCVD",1,8,"saturating",180,.7),
 F("V_NICOTINE","O_ASCVD",1,13,"saturating",180,.9),
 F("gly","O_T2DM",1,40,"saturating",120,.9), F("adi","O_T2DM",1,12,"saturating",120,.75),
 F("V_STEPS","O_MORT",-1,14,"saturating",90,.82), F("crf","O_MORT",-1,30,"saturating",180,.9),
 F("infl","O_MORT",1,10,"saturating",180,.65), F("O_ASCVD","O_MORT",1,16,"linear",1,.85),
 F("mus","O_MORT",-1,12,"saturating",180,.7,"muscle mass -> survival (new)"),
 F("renr","O_MORT",-1,10,"saturating",180,.6,"renal reserve -> survival (new)"),
 F("bon","O_MORT",-1,6,"saturating",365,.5,"bone reserve -> fracture-related survival (new)"),
 F("infl","O_BIOAGE",1,18,"saturating",180,.65), F("crf","O_BIOAGE",-1,16,"saturating",180,.6),
 F("gly","O_BIOAGE",1,10,"saturating",180,.55),
 F("allo","O_ENERGY",-1,18,"saturating",7,.6,"allostatic load drains vitality (folds sim R_ANS/R_STR)"),
 F("sld","O_ENERGY",-1,24,"saturating",3,.75),
 F("oxd","O_ENERGY",1,12,"saturating",14,.6,"oxygen delivery -> energy (new)"),
 F("iron","O_ENERGY",1,10,"saturating",21,.6,"iron repletion -> energy / less fatigue (new)"),
 F("micr","O_ENERGY",1,8,"saturating",21,.5,"micronutrients -> energy (new)"),
]

# ---- 3 patients (extend sim's to 15 reservoirs) ----
PATIENTS = [
 {"name":"Omar H.","meta":"44M · UAE · Moderate cardiometabolic risk",
  "res":{"crf":35,"allo":50,"sld":55,"gly":60,"adi":62,"vbp":55,"ath":58,"infl":52,"hepf":55,
         "mus":48,"bon":55,"renr":62,"micr":45,"oxd":58,"iron":52}},
 {"name":"Fatima S.","meta":"58F · GCC · High cardiovascular risk",
  "res":{"crf":30,"allo":62,"sld":60,"gly":68,"adi":64,"vbp":72,"ath":60,"infl":58,"hepf":58,
         "mus":40,"bon":42,"renr":55,"micr":40,"oxd":52,"iron":44}},
 {"name":"Daniel R.","meta":"39M · Western · Early pre-diabetes trajectory",
  "res":{"crf":55,"allo":45,"sld":42,"gly":48,"adi":50,"vbp":40,"ath":45,"infl":38,"hepf":48,
         "mus":58,"bon":60,"renr":70,"micr":58,"oxd":62,"iron":55}},
]

MODEL = {
 "_meta":{
   "title":"PureScore reservoir-flow model (BIOMONIAC)",
   "basis":"Runnable MONIAC companion to Doc 04. 15 canonical reservoirs (codes/polarity/feeds anchored to calc-graph.json), 5 transfer-function shapes, evidence-tagged flows. Ported from the 9-reservoir cardiometabolic sim and expanded to 15. ILLUSTRATIVE / expert-prior — calibrate vs Doc 13/14 before any clinical use.",
   "shapes":["linear","saturating","log","threshold","j_curve"],
   "counts":{"reservoirs":len(RES),"valves":len(VALVES)+len(RX_VALVES),"flows":len(FLOWS),"outcomes":len(OUT)},
 },
 "reservoirs":RES, "valves":VALVES, "rx_valves":RX_VALVES, "outcomes":OUT,
 "flows":FLOWS, "patients":PATIENTS,
}

# ---- validate: every flow endpoint is a known node ----
res_ids={r["id"] for r in RES}
valve_ids={v["id"] for v in VALVES+RX_VALVES}
out_ids={o["id"] for o in OUT}
nodes=res_ids|valve_ids|out_ids
errs=[]
for f in FLOWS:
    if f["src"] not in nodes: errs.append("unknown src %s"%f["src"])
    if f["tgt"] not in nodes: errs.append("unknown tgt %s"%f["tgt"])
    if f["sh"] not in MODEL["_meta"]["shapes"]: errs.append("unknown shape %s"%f["sh"])
for p in PATIENTS:
    miss=res_ids-set(p["res"]);
    if miss: errs.append("patient %s missing reservoirs %s"%(p["name"],sorted(miss)))
if errs:
    raise SystemExit("VALIDATION FAILED:\n  "+"\n  ".join(errs))

out=os.path.join(HERE,"reservoir-flows.json")
json.dump(MODEL, open(out,"w",encoding="utf-8"), ensure_ascii=False, indent=1)
print("OK wrote %s — %d reservoirs · %d valves · %d flows · %d outcomes · %d patients" % (
    out, len(RES), len(VALVES)+len(RX_VALVES), len(FLOWS), len(OUT), len(PATIENTS)))
