#!/usr/bin/env python3
"""Render the PureScore architecture + formula as a static PNG (cairosvg)."""
import math, cairosvg

W, H = 1680, 2120
INK="#e8edf7"; MUT="#9aa7c7"; BG1="#0e1320"; BG2="#0b101c"
LINE="#2c3858"; ACC="#5b8cff"; TEAL="#2bb9c4"
GREEN="#2e9e5b"; YEL="#e0a82e"; RED="#d1495b"; PURP="#7a86ff"; ASSET="#36b37e"

s=[]
def add(x): s.append(x)

def esc(t): return (t.replace("&","&amp;").replace("<","&lt;").replace(">","&gt;"))

def text(x,y,t,size=18,col=INK,anchor="start",weight="normal",mono=False,opacity=1):
    fam="DejaVu Sans Mono,monospace" if mono else "DejaVu Sans,Arial,sans-serif"
    add(f'<text x="{x}" y="{y}" font-family="{fam}" font-size="{size}" fill="{col}" '
        f'text-anchor="{anchor}" font-weight="{weight}" opacity="{opacity}">{esc(t)}</text>')

def rect(x,y,w,h,rx=12,fill=BG2,stroke=LINE,sw=1.5,opacity=1,dash=None):
    d=f' stroke-dasharray="{dash}"' if dash else ''
    add(f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="{rx}" fill="{fill}" '
        f'stroke="{stroke}" stroke-width="{sw}" opacity="{opacity}"{d}/>')

def line(x1,y1,x2,y2,col=LINE,sw=2,dash=None,arrow=True,op=1):
    d=f' stroke-dasharray="{dash}"' if dash else ''
    m=' marker-end="url(#arr)"' if arrow else ''
    add(f'<line x1="{x1}" y1="{y1}" x2="{x2}" y2="{y2}" stroke="{col}" stroke-width="{sw}" opacity="{op}"{d}{m}/>')

def curve(x1,y1,x2,y2,col=LINE,sw=2,dash=None,arrow=False,op=1):
    mx,my=(x1+x2)/2,(y1+y2)/2
    d=f' stroke-dasharray="{dash}"' if dash else ''
    m=' marker-end="url(#arr)"' if arrow else ''
    add(f'<path d="M{x1},{y1} Q{mx},{my} {x2},{y2}" fill="none" stroke="{col}" stroke-width="{sw}" opacity="{op}"{d}{m}/>')

def lerp(a,b,t): return (a[0]+(b[0]-a[0])*t, a[1]+(b[1]-a[1])*t)

def jigsaw(x,y,w,h,tabs):
    c=[(0,0),(w,0),(w,h),(0,h)]
    k=min(w,h)*0.16
    d=f"M{x+c[0][0]},{y+c[0][1]}"
    for e in range(4):
        p0,p1,sg=c[e],c[(e+1)%4],tabs[e]
        if sg==0:
            d+=f" L{x+p1[0]},{y+p1[1]}"; continue
        dirv=(p1[0]-p0[0],p1[1]-p0[1])
        nrm=(dirv[1],-dirv[0]); nl=math.hypot(*nrm); nrm=(nrm[0]/nl,nrm[1]/nl)
        a=lerp(p0,p1,0.36); cc=lerp(p0,p1,0.64); pk=lerp(p0,p1,0.5)
        pk=(pk[0]+nrm[0]*sg*k, pk[1]+nrm[1]*sg*k)
        m1=lerp(p0,p1,0.42); m1=(m1[0]+nrm[0]*sg*k*1.5, m1[1]+nrm[1]*sg*k*1.5)
        m2=lerp(p0,p1,0.58); m2=(m2[0]+nrm[0]*sg*k*1.5, m2[1]+nrm[1]*sg*k*1.5)
        d+=(f" L{x+a[0]},{y+a[1]} Q{x+m1[0]},{y+m1[1]} {x+pk[0]},{y+pk[1]}"
            f" Q{x+m2[0]},{y+m2[1]} {x+cc[0]},{y+cc[1]} L{x+p1[0]},{y+p1[1]}")
    return d+" Z"

# ---------- header ----------
add(f'<svg xmlns="http://www.w3.org/2000/svg" width="{W}" height="{H}" viewBox="0 0 {W} {H}">')
add(f'<defs><marker id="arr" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" '
    f'orient="auto-start-reverse"><path d="M0,0 L10,5 L0,10 z" fill="#7f8cb0"/></marker>'
    f'<linearGradient id="bg" x1="0" y1="0" x2="0" y2="1"><stop offset="0" stop-color="{BG1}"/>'
    f'<stop offset="1" stop-color="{BG2}"/></linearGradient></defs>')
rect(0,0,W,H,rx=0,fill="url(#bg)",stroke="none")
text(50,62,"PureScore — Overall Architecture & Scoring Formula",34,INK,weight="bold")
text(50,96,"Wellness-grade · clinician-in-the-loop · 12 pillars · MONIAC reservoir dynamics · illustrative bands (re-verify before real use)",17,MUT)
line(50,116,W-50,116,col=LINE,sw=1.5,arrow=False)

# ============================================================
# PANEL A — ARCHITECTURE
# ============================================================
text(50,158,"A.  DATA  →  MARKER RISK  →  PILLARS (the puzzle)  →  PureScore  →  OUTPUTS",20,TEAL,weight="bold")

# ---- left: input sources ----
inputs=[("Biomarkers","labs: ApoB, HbA1c, eGFR, hsCRP, TSH …"),
        ("Wearables / devices","resting HR, HRV, sleep, SpO2, steps, CGM"),
        ("Lifestyle / persona / PRO","diet, alcohol, smoking, PHQ-9, GAD-7, goals"),
        ("Clinical context","Dx flags D, Rx classes Mx, CAC, DEXA")]
ix,iy,iw,ih=50,200,300,74
for i,(t,sub) in enumerate(inputs):
    y=iy+i*(ih+16)
    rect(ix,y,iw,ih,fill="#16203a",stroke=ACC,sw=1.5)
    text(ix+16,y+30,t,18,INK,weight="bold")
    text(ix+16,y+54,sub,12.5,MUT)
# cohort + ranges + marker risk
cx0=ix+iw+70
rect(cx0,238,250,70,fill="#13233f",stroke=TEAL)
text(cx0+125,266,"Cohort  c(p)",16,INK,anchor="middle",weight="bold")
text(cx0+125,288,"age×sex×D×Mx + ref ranges",12.5,MUT,anchor="middle")
rect(cx0,330,250,70,fill="#13233f",stroke=TEAL)
text(cx0+125,358,"Marker risk  r_i",16,INK,anchor="middle",weight="bold",mono=False)
text(cx0+125,380,"two-sided clinical band map",12.5,MUT,anchor="middle")
rect(cx0,422,250,70,fill="#13233f",stroke=TEAL)
text(cx0+125,450,"Literature fallback",16,INK,anchor="middle",weight="bold")
text(cx0+125,472,"NHANES median + low confidence",12,MUT,anchor="middle")
# arrows inputs -> cohort/markerrisk
for i in range(4):
    y=iy+i*(ih+16)+ih/2
    curve(ix+iw,y,cx0,360,col="#46527a",sw=1.6,arrow=True,op=.8)
line(cx0+125,308,cx0+125,330,col="#46527a",sw=1.6,arrow=True)
line(cx0+125,400,cx0+125,422,col="#46527a",sw=1.6,arrow=True)

# ---- center: radial puzzle ----
PCX,PCY,RING=920,520,250
PW,PH=128,76
pillars=[("CV","Cardiovascular",GREEN),("MET","Metabolic",GREEN),("REN","Renal",RED),
         ("HEP","Hepatic",GREEN),("INF","Inflammation",YEL),("HEM","Hematologic",GREEN),
         ("ENDO","Endocrine",GREEN),("BCM","Body/MSK",GREEN),("NUT","Nutrition",GREEN),
         ("SLP","Sleep",YEL),("FIT","Fitness",GREEN),("MCS","Mental/Cog",GREEN)]
pos={}
for i,(pid,nm,col) in enumerate(pillars):
    ang=-math.pi/2 + i*2*math.pi/12
    pos[pid]=(PCX+RING*math.cos(ang), PCY+RING*math.sin(ang))

# reservoirs (outer) with dashed coupling
reservoirs=[("SLD",["SLP","MCS","MET","CV","INF"],PURP),
            ("ATH",["CV"],PURP),
            ("INFL",["INF","CV","MET","HEP","MCS"],PURP),
            ("GLY",["MET","CV","REN","HEP"],PURP),
            ("ALLO",["MCS","ENDO","CV","SLP"],PURP),
            ("CRF",["FIT","CV","MET"],ASSET),
            ("MUS",["BCM","FIT","MET"],ASSET)]
rpos={}
for i,(rid,feeds,col) in enumerate(reservoirs):
    ang=-math.pi/2 + (i+0.5)*2*math.pi/7
    rpos[rid]=(PCX+(RING+170)*math.cos(ang), PCY+(RING+150)*math.sin(ang))
# dashed interference edges first (behind)
for rid,feeds,col in reservoirs:
    a=rpos[rid]
    for pid in feeds:
        b=pos[pid]
        curve(a[0],a[1],b[0],b[1],col=col,sw=1.2,dash="3 5",op=.22,arrow=False)
# flow arrows pillar->hub
for pid,nm,col in pillars:
    a=pos[pid]; dx=PCX-a[0]; dy=PCY-a[1]; L=math.hypot(dx,dy)
    sx,sy=a[0]+dx/L*52,a[1]+dy/L*34; ex,ey=PCX-dx/L*100,PCY-dy/L*100
    line(sx,sy,ex,ey,col=col,sw=2.4,arrow=True,op=.85)
# pillar pieces
for i,(pid,nm,col) in enumerate(pillars):
    a=pos[pid]; x=a[0]-PW/2; y=a[1]-PH/2
    tabs=[(1 if i%2 else -1),(-1 if i%2 else 1),(1 if i%2 else -1),(-1 if i%2 else 1)]
    add(f'<path d="{jigsaw(x,y,PW,PH,tabs)}" fill="{col}" fill-opacity="0.20" stroke="{col}" stroke-width="2.6"/>')
    text(a[0],a[1]-4,pid,17,"#fff",anchor="middle",weight="bold")
    text(a[0],a[1]+16,nm,11.5,"#cdd6ee",anchor="middle")
# reservoir nodes
for rid,feeds,col in reservoirs:
    a=rpos[rid]
    rect(a[0]-34,a[1]-17,68,34,rx=9,fill="#10162a",stroke=col,sw=1.4,opacity=.95)
    text(a[0],a[1]+5,rid,13,col,anchor="middle",weight="bold")
# central hub jigsaw
add(f'<path d="{jigsaw(PCX-104,PCY-104,208,208,[-1,1,-1,1])}" fill="#0c1f3c" stroke="{ACC}" stroke-width="3.5"/>')
text(PCX,PCY-46,"PureScore",18,MUT,anchor="middle")
text(PCX,PCY+22,"88",70,"#fff",anchor="middle",weight="bold")
text(PCX,PCY+56,"0–100",14,MUT,anchor="middle")
text(PCX,PCY+170,"12 pillar pieces  →  worst-sensitive δ-power-mean  →  critical-cascade cap",13,MUT,anchor="middle")
# arrow from marker-risk block into the ring
line(cx0+250,360,pos["NUT"][0]-PW/2-8,pos["NUT"][1],col="#46527a",sw=2,arrow=True)

# ---- right: outputs ----
ox=1340
outs=[("PATIENT",GREEN,["PureScore + 12 pillars","Daily top-5 easiest nudges","Care / nutrition / exercise plan"]),
      ("CLINICIAN",ACC,["FINDRISC · ASCVD · SCORE2","KDIGO · FIB-4 · FRAX · BioAge","Critical → escalation pathway"]),
      ("PAYER (gated)",YEL,["Pillar stack-rank percentiles","Actuarial pricing —","heavy fairness/legal guardrails"])]
oy=200
for t,col,rows in outs:
    rect(ox,oy,290,118,fill="#141d34",stroke=col,sw=1.6)
    text(ox+16,oy+30,t,16,col,weight="bold")
    for j,r in enumerate(rows):
        text(ox+16,oy+56+j*22,r,13,INK if "—" not in r else MUT)
    oy+=140
# arrow hub -> outputs
line(PCX+104,PCY,ox-10,oy-118-118-118-2-140+0+60,col="#46527a",sw=2,arrow=True)
line(PCX+150,PCY-40,ox-10,360,col="#46527a",sw=2,arrow=True)

# legend
ly=980
text(50,ly,"Legend:",15,MUT,weight="bold")
for i,(c,t) in enumerate([(GREEN,"green · on track (R<0.30)"),(YEL,"yellow · caution (0.30–0.60)"),
                          (RED,"red / critical (≥0.60 or critical-marker red)"),(PURP,"burden reservoir"),(ASSET,"asset reservoir")]):
    x=130+i*300
    add(f'<circle cx="{x}" cy="{ly-5}" r="7" fill="{c}"/>')
    text(x+14,ly,t,13.5,MUT)
text(130,ly+26,"⟶ data/score flow into PureScore hub        ⋯ MONIAC reservoir interference (cross-pillar coupling)",13.5,MUT)

line(50,1030,W-50,1030,col=LINE,sw=1.5,arrow=False)

# ============================================================
# PANEL B — FORMULA
# ============================================================
text(50,1072,"B.  THE SCORING FORMULA  (marker → pillar → PureScore)",20,TEAL,weight="bold")

def fbox(x,y,w,h,title,lines,col=ACC):
    rect(x,y,w,h,fill="#121a2e",stroke=col,sw=1.6)
    text(x+18,y+30,title,16,col,weight="bold")
    yy=y+58
    for ln,big in lines:
        text(x+18,yy,ln,15 if big else 13.5,INK if big else MUT,mono=big)
        yy+=26 if big else 22

col1=50; col2=600; col3=1150; bw=500
y0=1100
fbox(col1,y0,bw,236,"① Marker risk  r_i  (two-sided clinical bands)",
 [("optimal[L,U]→0   yellow→0.15   red→0.50",False),
  ("r_i^clin: 0  in [L,U]",True),
  ("   0.15·(x−U)/(Uy−U)   U<x≤Uy",True),
  ("   0.15+0.35·(x−Uy)/(Ur−Uy)",True),
  ("   0.50+0.50(1−e^−(x−Ur)/σ)  x>Ur",True),
  ("safety anchor (clinical) dominates",False),
  ("r_i = max(r_i^clin , φ·r_i^cohort)",True)])

fbox(col2,y0,bw,236,"② Pillar risk  R_k  (worst-sensitive + reservoirs)",
 [("ŵ_i = w_i · confidence_i",True),
  ("",False),
  ("R_k^mark = ( Σ ŵ_i r_i^γ / Σ ŵ_i )^(1/γ)",True),
  ("                                  γ = 3",False),
  ("R_k = clamp( R_k^mark + ρ_k·B̃_k )",True),
  ("ρ_k=0.20   B̃_k = reservoir load (Doc 04)",False),
  ("S_k = 100·(1 − R_k)",True)])

fbox(col3,y0,bw,236,"③ Critical override (a red biomarker → critical)",
 [("status_k = CRITICAL if",False),
  ("  ∃ i: r_i≥0.5 ∧ i∈CriticalMarkers_k",True),
  ("  or  R_k ≥ 0.60",True),
  ("  (MCS: any suicidality ⇒ CRITICAL)",False),
  ("",False),
  ("if CRITICAL:  R_k ← max(R_k, 0.60)",True),
  ("emergency reds → clinician escalation",False)],col=RED)

y1=y0+262
fbox(col1,y1,bw,236,"④ PureScore  (personalized weights)",
 [("W_k = W_k^base·m_cohort·m_goal·m_acute",True),
  ("        (normalized so Σ W_k = 1)",False),
  ("",False),
  ("R_total = ( Σ W_k R_k^δ / Σ W_k )^(1/δ)",True),
  ("                                  δ = 2",False),
  ("PureScore° = 100·(1 − R_total)",True)])

fbox(col2,y1,bw,236,"⑤ Critical cascade  (safety cap)",
 [("if ∃ k: status_k = CRITICAL",True),
  ("   PureScore = min(PureScore°, 40)",True),
  ("   overall_status = CRITICAL",True),
  ("   emergency ⇒ escalation pathway",False),
  ("else PureScore = PureScore°",True),
  ("a single life-threatening value cannot",False),
  ("be averaged away by healthy pillars",False)],col=RED)

fbox(col3,y1,bw,236,"⑥ MONIAC reservoir dynamics  (memory)",
 [("B_j(t+Δ) = B_j(t) + Δ·[",True),
  ("   inflow_j(markers,behaviours)",True),
  ("   − λ_j·(B_j − B_j*)        time-decay",True),
  ("   + Σ κ_jl·sat(B_l) ]      interference",True),
  ("burdens vs assets · valves = interventions",False),
  ("sleep debt heals in days; CAC ~ permanent",False)],col=PURP)

# footer
line(50,y1+260,W-50,y1+260,col=LINE,sw=1.5,arrow=False)
text(50,y1+292,"Defaults: φ=0.60  γ=3  δ=2  ρ=0.20  R_crit=0.60  cap=40  ·  zone cuts r=0.15 / 0.50  ·  full spec: docs/purescore/  (Docs 03 & 04)",14,MUT)
text(50,y1+316,"Not a diagnostic device. Reference ranges illustrative & require clinician re-verification before any production use.",13.5,MUT)

add('</svg>')
svg="".join(s)
open("/home/user/pubmed-download/docs/purescore/assets/purescore-architecture.svg","w").write(svg)
cairosvg.svg2png(bytestring=svg.encode(),write_to="/home/user/pubmed-download/docs/purescore/assets/purescore-architecture.png",
                 output_width=W*2, output_height=H*2)
print("PNG + SVG written")
