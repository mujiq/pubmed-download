#!/usr/bin/env python3
"""Render the PureScore CEO executive deck (system narrative + 12-pillar drill-down) to PNG slides + a PDF."""
import math, os, zlib, cairosvg
from PIL import Image

def images_to_pdf(images, path):
    out=bytearray(); out+=b"%PDF-1.4\n%\xe2\xe3\xcf\xd3\n"; offsets={}; N=len(images)
    def wobj(num, body):
        offsets[num]=len(out); out.extend(f"{num} 0 obj\n".encode()); out.extend(body); out.extend(b"\nendobj\n")
    kids=" ".join(f"{3+i*3} 0 R" for i in range(N))
    wobj(1, b"<< /Type /Catalog /Pages 2 0 R >>")
    wobj(2, f"<< /Type /Pages /Kids [{kids}] /Count {N} >>".encode())
    for i,im in enumerate(images):
        im=im.convert("RGB"); w,h=im.size; comp=zlib.compress(im.tobytes(),6)
        page,content,img=3+i*3,4+i*3,5+i*3
        wobj(page, f"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 {w} {h}] /Resources << /XObject << /Im0 {img} 0 R >> >> /Contents {content} 0 R >>".encode())
        cs=f"q {w} 0 0 {h} 0 0 cm /Im0 Do Q".encode()
        wobj(content, b"<< /Length "+str(len(cs)).encode()+b" >>\nstream\n"+cs+b"\nendstream")
        hdr=f"<< /Type /XObject /Subtype /Image /Width {w} /Height {h} /ColorSpace /DeviceRGB /BitsPerComponent 8 /Filter /FlateDecode /Length {len(comp)} >>".encode()
        wobj(img, hdr+b"\nstream\n"+comp+b"\nendstream")
    xref=len(out); mx=2+N*3
    out.extend(f"xref\n0 {mx+1}\n".encode()); out.extend(b"0000000000 65535 f \n")
    for num in range(1,mx+1): out.extend(f"{offsets[num]:010d} 00000 n \n".encode())
    out.extend(f"trailer\n<< /Size {mx+1} /Root 1 0 R >>\nstartxref\n{xref}\n%%EOF".encode())
    open(path,"wb").write(out)

W, H = 1920, 1080
INK="#eaf0fb"; MUT="#9aa7c7"; DIM="#6f7da0"; BG1="#0c111d"; BG2="#0a0e18"
LINE="#2c3858"; ACC="#5b8cff"; TEAL="#2bb9c4"; GOLD="#e0a82e"
GREEN="#2e9e5b"; YEL="#e0a82e"; RED="#d1495b"; PURP="#7a86ff"; ASSET="#36b37e"; CY="#7fd1ff"

OUT=os.path.dirname(os.path.abspath(__file__)); SD=os.path.join(OUT,"deck"); os.makedirs(SD,exist_ok=True)
s=[]; paths=[]
def add(x): s.append(x)
def esc(t): return str(t).replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")

def text(x,y,t,size=18,col=INK,anchor="start",weight="normal",mono=False,op=1,ital=False):
    fam="DejaVu Sans Mono,monospace" if mono else "DejaVu Sans,Arial,sans-serif"
    st=' font-style="italic"' if ital else ''
    add(f'<text x="{x}" y="{y}" font-family="{fam}" font-size="{size}" fill="{col}" text-anchor="{anchor}" font-weight="{weight}" opacity="{op}"{st}>{esc(t)}</text>')
def rect(x,y,w,h,rx=14,fill=BG2,stroke=LINE,sw=1.5,op=1,dash=None):
    d=f' stroke-dasharray="{dash}"' if dash else ''
    add(f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="{rx}" fill="{fill}" stroke="{stroke}" stroke-width="{sw}" opacity="{op}"{d}/>')
def line(x1,y1,x2,y2,col=LINE,sw=2,dash=None,arrow=False,op=1):
    d=f' stroke-dasharray="{dash}"' if dash else ''
    m=' marker-end="url(#arr)"' if arrow else ''
    add(f'<line x1="{x1}" y1="{y1}" x2="{x2}" y2="{y2}" stroke="{col}" stroke-width="{sw}" opacity="{op}"{d}{m}/>')
def curve(x1,y1,x2,y2,col=LINE,sw=2,dash=None,arrow=False,op=1,bow=0.0):
    mx,my=(x1+x2)/2,(y1+y2)/2
    nx,ny=-(y2-y1),(x2-x1); nl=math.hypot(nx,ny) or 1; mx+=nx/nl*bow; my+=ny/nl*bow
    d=f' stroke-dasharray="{dash}"' if dash else ''
    m=' marker-end="url(#arr)"' if arrow else ''
    add(f'<path d="M{x1},{y1} Q{mx},{my} {x2},{y2}" fill="none" stroke="{col}" stroke-width="{sw}" opacity="{op}"{d}{m}/>')
def circle(cx,cy,r,fill="none",stroke=LINE,sw=2,op=1):
    add(f'<circle cx="{cx}" cy="{cy}" r="{r}" fill="{fill}" stroke="{stroke}" stroke-width="{sw}" opacity="{op}"/>')
def lerp(a,b,t): return (a[0]+(b[0]-a[0])*t, a[1]+(b[1]-a[1])*t)
def jigsaw(x,y,w,h,tabs):
    c=[(0,0),(w,0),(w,h),(0,h)]; k=min(w,h)*0.18; d=f"M{x+c[0][0]},{y+c[0][1]}"
    for e in range(4):
        p0,p1,sg=c[e],c[(e+1)%4],tabs[e]
        if sg==0: d+=f" L{x+p1[0]},{y+p1[1]}"; continue
        dv=(p1[0]-p0[0],p1[1]-p0[1]); nrm=(dv[1],-dv[0]); nl=math.hypot(*nrm); nrm=(nrm[0]/nl,nrm[1]/nl)
        a=lerp(p0,p1,0.36); cc=lerp(p0,p1,0.64); pk=lerp(p0,p1,0.5); pk=(pk[0]+nrm[0]*sg*k,pk[1]+nrm[1]*sg*k)
        m1=lerp(p0,p1,0.42); m1=(m1[0]+nrm[0]*sg*k*1.5,m1[1]+nrm[1]*sg*k*1.5)
        m2=lerp(p0,p1,0.58); m2=(m2[0]+nrm[0]*sg*k*1.5,m2[1]+nrm[1]*sg*k*1.5)
        d+=(f" L{x+a[0]},{y+a[1]} Q{x+m1[0]},{y+m1[1]} {x+pk[0]},{y+pk[1]} Q{x+m2[0]},{y+m2[1]} {x+cc[0]},{y+cc[1]} L{x+p1[0]},{y+p1[1]}")
    return d+" Z"
def chip(x,y,t,col=ACC,fill="#141c30",size=18):
    w=len(t)*size*0.62+34; rect(x,y,w,size+18,rx=(size+18)/2,fill=fill,stroke=col,sw=1.5)
    text(x+17,y+size+3,t,size,col,weight="bold"); return w
def piece(cx,cy,w,h,tabs,col,label,sub=None,score=None,op=0.22):
    add(f'<path d="{jigsaw(cx-w/2,cy-h/2,w,h,tabs)}" fill="{col}" fill-opacity="{op}" stroke="{col}" stroke-width="3"/>')
    text(cx,cy-(6 if sub else -8),label,30 if not sub else 28,"#fff",anchor="middle",weight="bold")
    if sub: text(cx,cy+22,sub,16,"#cdd6ee",anchor="middle")
    if score is not None: text(cx,cy+24,score,20,col,anchor="middle",weight="bold")

TABS=[(-1,1,-1,1),(1,-1,1,-1)]
def radial_puzzle(cx,cy,R,pw=150,ph=92,center="PureScore",cnum="88"):
    cols=[RED,GOLD,ACC,GREEN,"#e0673e","#c0506b",PURP,TEAL,"#7bbf5a","#6a7bd6","#3fb6a8","#d98ec0"]
    ids=["CV","MET","REN","HEP","INF","HEM","ENDO","BCM","NUT","SLP","FIT","MCS"]
    for i in range(12):
        a=-math.pi/2+i*2*math.pi/12; px,py=cx+R*math.cos(a),cy+R*math.sin(a)
        dx,dy=cx-px,cy-py; L=math.hypot(dx,dy)
        line(px+dx/L*54,py+dy/L*40,cx-dx/L*120,cy-dy/L*120,col=cols[i],sw=2,op=.5,arrow=True)
        piece(px,py,pw,ph,TABS[i%2],cols[i],ids[i])
    add(f'<path d="{jigsaw(cx-120,cy-120,240,240,[-1,1,-1,1])}" fill="#0f2138" stroke="{ACC}" stroke-width="4"/>')
    text(cx,cy-44,center,22,"#9fb0d8",anchor="middle")
    text(cx,cy+22,cnum,80,"#fff",anchor="middle",weight="bold")
    text(cx,cy+60,"0–100",18,MUT,anchor="middle")

def start():
    s.clear()
    add(f'<svg xmlns="http://www.w3.org/2000/svg" width="{W}" height="{H}" viewBox="0 0 {W} {H}">')
    add(f'<defs><marker id="arr" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M0,0 L10,5 L0,10 z" fill="#7f8cb0"/></marker>'
        f'<linearGradient id="bg" x1="0" y1="0" x2="1" y2="1"><stop offset="0" stop-color="{BG1}"/><stop offset="1" stop-color="{BG2}"/></linearGradient>'
        f'<radialGradient id="halo" cx="0.5" cy="0.5" r="0.5"><stop offset="0" stop-color="{ACC}" stop-opacity="0.18"/><stop offset="1" stop-color="{ACC}" stop-opacity="0"/></radialGradient></defs>')
    rect(0,0,W,H,rx=0,fill="url(#bg)",stroke="none")
def footer(n):
    line(80,H-70,W-80,H-70,col=LINE,sw=1.2)
    text(80,H-40,"PureScore  ·  Confidential",17,DIM)
    text(W/2,H-40,"Illustrative design — not yet clinically validated (Doc 00 · Babylon discipline)",16,DIM,anchor="middle")
    text(W-80,H-40,f"{n:02d} / {TOTAL:02d}",17,DIM,anchor="end")
def head(kicker,title,subtitle=None,kcol=TEAL):
    chip(80,56,kicker,kcol)
    text(80,150,title,54,INK,weight="bold")
    if subtitle: text(80,200,subtitle,26,MUT)
    line(80,228,W-80,228,col=LINE,sw=1.5)
def bullet(x,y,h,sub=None,dot=ACC,size=29):
    add(f'<circle cx="{x+9}" cy="{y-10}" r="8" fill="{dot}"/>')
    text(x+34,y,h,size,INK,weight="bold")
    if sub: text(x+34,y+33,sub,20,MUT)
def end(n):
    footer(n); add("</svg>")
    p=os.path.join(SD,f"slide{n:02d}.png")
    cairosvg.svg2png(bytestring="".join(s).encode(),write_to=p,output_width=W,output_height=H)
    paths.append(p)

# ============================================================ content
SLIDES=[]
def slide(fn): SLIDES.append(fn); return fn

@slide
def s01(n):
    start()
    add(f'<rect x="0" y="0" width="{W}" height="{H}" fill="url(#halo)"/>')
    radial_puzzle(1430,540,300,pw=150,ph=92)
    chip(80,150,"EXECUTIVE BRIEFING · 2026",GOLD)
    text(80,330,"PureScore",96,INK,weight="bold")
    text(80,410,"The AI doctor that compounds.",44,CY,weight="bold")
    text(80,486,"A wellness-grade, clinician-in-the-loop health-intelligence engine:",26,MUT)
    text(80,524,"every patient is scored, watched, and coached — and the system gets",26,MUT)
    text(80,562,"sharper with every measurement it sees.",26,MUT)
    bullet(80,650,"12 health pillars → one trusted score",None,ACC)
    bullet(80,706,"Catches disease while it is still incubating",None,GREEN)
    bullet(80,762,"Localized for the UAE, governed by evidence",None,GOLD)
    bullet(80,818,"Self-improving via a data + reinforcement loop",None,PURP)
    end(n)

@slide
def s02(n):
    start(); head("01 · THE PROBLEM","Why health scores fail","A single number, naively built, is confidently wrong.",RED)
    bullet(90,320,"Context blindness","The same value means opposite things: resting HR 50 is elite in an athlete, dangerous on a β-blocker.",RED)
    bullet(90,420,"Too late","Level-based thresholds only fire once disease has crossed a line — no early warning.",RED)
    bullet(90,520,"Over-confident & opaque","One number, no error bars, no ‘says who?’ — clinicians can't trust or audit it.",RED)
    bullet(90,620,"Not personal, not local","Western adult averages mis-score the UAE's South-Asian-majority population.",RED)
    rect(1080,300,760,470,rx=20,fill="#1a0e16",stroke=RED,sw=2)
    text(1460,360,"The Babylon lesson",30,RED,anchor="middle",weight="bold")
    text(1460,408,"A celebrated ‘AI doctor’ over-promised,",22,MUT,anchor="middle")
    text(1460,440,"under-validated, and lost clinical trust.",22,MUT,anchor="middle")
    text(1460,520,"❝ No claim ships ahead",26,INK,anchor="middle",ital=True)
    text(1460,558,"of its evidence. ❞",26,INK,anchor="middle",ital=True)
    text(1460,650,"PureScore is built as the",20,MUT,anchor="middle")
    text(1460,680,"deliberate antidote.",20,CY,anchor="middle",weight="bold")
    end(n)

@slide
def s03(n):
    start(); head("02 · THE SOLUTION","Twelve pieces. One picture.","Each health domain is a puzzle piece that locks into the whole.",CY)
    radial_puzzle(1380,610,300)
    bullet(90,330,"The puzzle","12 pillars (heart, metabolic, kidney, liver…) interlock — a gap in one weakens the whole.",ACC)
    bullet(90,430,"The score","Worst-sensitive: a single life-threatening value can't be averaged away by good numbers elsewhere.",ACC)
    bullet(90,530,"The memory","MONIAC ‘reservoirs’ store accumulated burden — a chronic debt shows even on a good day.",ACC)
    bullet(90,630,"The trust layer","The score ships with its own uncertainty, direction, and citations.",ACC)
    bullet(90,730,"The loop","Personal baselines + outcomes feed back — the engine learns each patient and the whole cohort.",PURP)
    end(n)

@slide
def s04(n):
    start(); head("03 · INGESTION","The patient at the centre","Every available signal, scored against the best reference — with honest coverage.")
    cx,cy=1430,560; circle(cx,cy,90,fill="#0f2138",stroke=ACC,sw=3); text(cx,cy-6,"PATIENT",24,"#fff",anchor="middle",weight="bold"); text(cx,cy+28,"+ baseline",16,MUT,anchor="middle")
    srcs=[("Labs / biomarkers",GREEN),("Wearables & devices",ACC),("Lifestyle / PRO",GOLD),("Clinical history",PURP),("Genetics / family",RED),("Context: sex·age·ethnicity",TEAL)]
    for i,(t,c) in enumerate(srcs):
        a=-math.pi/2+i*2*math.pi/6; px,py=cx+260*math.cos(a),cy+230*math.sin(a)
        rect(px-130,py-30,260,60,rx=12,fill="#10182a",stroke=c,sw=1.6); text(px,py+6,t,17,c,anchor="middle",weight="bold")
        line(px,py+(28 if py<cy else -28),cx+(a and 70*math.cos(a)),cy+70*math.sin(a),col=c,sw=1.6,op=.5,arrow=True)
    bullet(90,330,"Use the patient's own data when present","Fall back to literature/cohort references when absent — a score is always produced.",ACC)
    bullet(90,430,"Tiered & coverage-aware","Each input has a tier and recency; missing data lowers Confidence, never fabricates it.",ACC)
    bullet(90,530,"Organ-inventory & context aware","Only scores what applies; captures sex-at-birth, hormones, life-stage, ethnicity.",ACC)
    bullet(90,630,"Privacy by design","Sensitive data (genetics, pregnancy) is minimized, gated, and never used to price or deny.",GREEN)
    end(n)

@slide
def s05(n):
    start(); head("04 · SCORING CORE","The 12-pillar puzzle → PureScore","Marker → pillar → score, with a critical cascade and reservoir memory.")
    radial_puzzle(1380,600,290)
    bullet(90,320,"Marker risk r","Each value mapped to 0→1 risk against a green / yellow / red band (clinical + cohort).",ACC)
    bullet(90,408,"Pillar risk Rₖ","γ-power-mean of its markers — the worst marker dominates (fix the binding constraint).",ACC)
    bullet(90,496,"PureScore","100·(1 − δ-power-mean of pillars) — worst-sensitive across domains.",ACC)
    bullet(90,584,"Critical cascade","One red critical marker caps the score at 40 → cannot be averaged away.",RED)
    bullet(90,672,"Reservoir memory","Sleep debt, atherogenic/glycemic/inflammatory burden inject cross-pillar latency.",PURP)
    rect(90,728,760,150,rx=14,fill="#0d1322",stroke=LINE)
    text(110,772,"PureScore = 100 · ( 1 − R_total )",26,CY,mono=True,weight="bold")
    text(110,812,"R_total = ( Σ Wₖ·Rₖ^δ / ΣWₖ )^(1/δ)     [δ=2, worst-sensitive]",19,MUT,mono=True)
    text(110,848,"if any critical red →  PureScore ≤ 40  (emergency escalation)",19,RED,mono=True)
    end(n)

@slide
def s06(n):
    start(); head("05 · THE TRUST LAYER","A score with error bars","The number never travels alone — it carries its own honesty.",GREEN)
    dims=[("Confidence","is the data trustworthy?",GREEN),("Data sufficiency","measured vs imputed",GOLD),("Criticality","independent red-flag badge",RED),
          ("Trajectory","improving / worsening",ACC),("Early-warning","Watch · Advisory · Alert",CY),("Representativeness","does the cohort fit you?",PURP),
          ("Skew / robustness","heavy-tailed handling",TEAL),("Volatility","measurement stability",GOLD),("Modifiability","how much is in your control",GREEN)]
    for i,(t,sub,c) in enumerate(dims):
        col=i%3; row=i//3; x=90+col*590; y=320+row*150
        rect(x,y,560,128,rx=16,fill="#10182a",stroke=c,sw=1.8)
        text(x+26,y+50,t,28,c,weight="bold"); text(x+26,y+92,sub,21,MUT)
    text(90,860,"A ‘72 from fresh labs’ and a ‘72 from imputation’ no longer look identical.",24,INK,ital=True)
    end(n)

@slide
def s07(n):
    start(); head("06 · EARLY WARNING","Catch it while it's incubating","Stop waiting for thresholds — watch personal baselines and motion.",CY)
    bullet(90,330,"Personal baseline (z-score)","Your resting HR 52→60 matters even though both are ‘green’.",CY)
    bullet(90,430,"Within-green drift & forecast","Projects days-to-threshold before a band is ever crossed.",CY)
    bullet(90,530,"Multivariate anomaly","Many small simultaneous shifts — early sepsis, decompensation — caught together.",CY)
    bullet(90,630,"Tiered & buffered","Watch (internal) → Advisory (gentle) → Alert (clinician). No alarm fatigue.",CY)
    # mini trend chart
    gx,gy,gw,gh=1080,330,760,470; rect(gx,gy,gw,gh,rx=18,fill="#0d1322",stroke=LINE)
    line(gx+40,gy+gh-60,gx+gw-30,gy+gh-60,col=LINE,sw=1.2); line(gx+40,gy+40,gx+40,gy+gh-60,col=LINE,sw=1.2)
    rect(gx+40,gy+60,gw-70,110,rx=0,fill="#1d2a18",stroke="none",op=.5); text(gx+gw-40,gy+96,"yellow",15,YEL,anchor="end")
    pts=[(0,.78),(.18,.74),(.34,.7),(.5,.64),(.66,.57),(.82,.48),(1,.36)]
    d="M"+" L".join(f"{gx+40+(gw-80)*px},{gy+40+(gh-110)*(1-py)}" for px,py in pts)
    add(f'<path d="{d}" fill="none" stroke="{CY}" stroke-width="3.5"/>')
    add(f'<circle cx="{gx+40+(gw-80)}" cy="{gy+40+(gh-110)*(1-.36)}" r="7" fill="{CY}"/>')
    text(gx+gw-50,gy+gh-150,"~172 d to yellow",18,CY,anchor="end",weight="bold")
    text(gx+60,gy+34,"personal baseline drifting toward risk — flagged early",17,MUT)
    end(n)

@slide
def s08(n):
    start(); head("07 · CONTEXT-AWARE","Same value, right meaning","The engine reads each marker through the patient's real context.",GOLD)
    ctx=[("On medication","Controlled = ‘MANAGED’, not cured; β-blocker stops corrupting fitness.",GOLD),
         ("Sex & hormones","Hgb follows the hormonal milieu — correct for HRT, pregnancy, menopause.",PURP),
         ("Age & life-stage","J-curve for frail elders; pregnancy trimester frames; never under-treat.",ACC),
         ("Ethnicity (UAE)","WHO South-Asian BMI/waist cut-points; race-free eGFR — context, never penalty.",TEAL),
         ("Ramadan","Med-timing & hydration; absolute break-the-fast safety rule.",GREEN)]
    for i,(t,sub,c) in enumerate(ctx):
        y=320+i*112; rect(90,y,1740,96,rx=14,fill="#10182a",stroke=c,sw=1.6)
        text(120,y+44,t,28,c,weight="bold"); text(120,y+80,sub,21,MUT)
    end(n)

@slide
def s09(n):
    start(); head("08 · EVIDENCE & PROVENANCE","The diesel-engine ignition","Cold-crank on guidelines, warm up on cohorts, run on the patient.",GOLD)
    stages=[("CRANK","Clinical guideline anchor","cold start · no patient data",GOLD,0.62),
            ("WARM-UP","Real-world cohort range","some data · cohort percentile",ACC,0.5),
            ("RUNNING","Personal baseline z-score","established · self-sustaining",GREEN,0.2)]
    x=1060
    for i,(t,h,sub,c,th) in enumerate(stages):
        y=320+i*155; rect(x,y,780,130,rx=16,fill="#10182a",stroke=c,sw=1.8)
        text(x+28,y+50,t,26,c,weight="bold"); text(x+150,y+50,h,24,INK,weight="bold"); text(x+28,y+92,sub,19,MUT)
        rect(x+540,y+30,210,24,rx=12,fill="#0a0e18",stroke=c); rect(x+540,y+30,210*th,24,rx=12,fill=c,stroke="none",op=.55)
        text(x+645,y+92,f"θ ≈ {th:.2f}",17,c,anchor="middle",mono=True)
        if i<2: line(x+390,y+130,x+390,y+155,col=MUT,sw=2,arrow=True)
    bullet(90,340,"Every band cites its evidence","42 registry entries: ADA, KDIGO, ESC, WHO, IDF + UAE bodies (MoHAP, DHA, IDF-DAR).",GOLD)
    bullet(90,440,"Weights shift with data","Empirical-Bayes: the more the patient measures, the more personal the reference.",ACC)
    bullet(90,540,"Safety never leaves the crank","Acute-danger anchors stay absolute regardless of personalization.",RED)
    bullet(90,640,"Kept current by a crawler","Guideline changes are detected, staged, and human-approved before anything moves.",PURP)
    bullet(90,740,"Auditable","A clinician can ask any number: ‘says who, from when?’ — and get the citation.",GREEN)
    end(n)

@slide
def s10(n):
    start(); head("09 · THE NUDGE ENGINE","Five easiest wins, attributed","Impact-ranked daily actions — honest about what each is worth.",GREEN)
    nudges=[("Cut refined carbs","MET · effort ●●○○○","+5.7",GREEN),
            ("2× resistance training","BCM · effort ●●●○○","+3.4",GREEN),
            ("Protect 7.5–8 h sleep","SLP · effort ●●○○○","+1.6",GREEN),
            ("+1,500 steps / day","FIT · effort ●○○○○","+1.4",GREEN),
            ("Book clinician review","CLINICAL · genetic ApoB","refer",CY)]
    for i,(t,sub,d,c) in enumerate(nudges):
        y=320+i*108; rect(1040,y,800,92,rx=14,fill="#10182a",stroke=c,sw=1.6)
        text(1070,y+40,t,26,INK,weight="bold"); text(1070,y+74,sub,19,MUT)
        text(1810,y+56,d,30,c,anchor="end",weight="bold")
    bullet(90,340,"Ranked by ease × impact","U = [ΔPureScore·evidence]·adherence·ease — high-impact actions you won't do score zero.",GREEN)
    bullet(90,448,"Exact, honest impact","ΔPureScore recomputed through the engine — diminishing returns in green, no false hope.",GREEN)
    bullet(90,556,"Modifiability-gated","Lifestyle can't move a genetic marker (FH ApoB) → it routes to a clinician instead.",CY)
    bullet(90,664,"Safe & local","CKD protein cap, pregnancy/lactation caveats, Ramadan timing baked in.",GOLD)
    end(n)

@slide
def s11(n):
    start(); head("10 · THE ECOSYSTEM","Built around the clinician","PureScore orchestrates the entities; humans hold the critical decisions.",ACC)
    cx,cy=1400,560; add(f'<path d="{jigsaw(cx-95,cy-95,190,190,[-1,1,-1,1])}" fill="#0f2138" stroke="{ACC}" stroke-width="3"/>')
    text(cx,cy-6,"PureScore",22,"#fff",anchor="middle",weight="bold"); text(cx,cy+26,"engine",16,MUT,anchor="middle")
    ents=[("Clinician",GREEN,"approves every critical"),("Patient",ACC,"daily score + plan"),("Lab / Diagnostics",TEAL,"orders & results"),
          ("Pharmacy",GOLD,"adherence loop"),("Payer  🔒 firewall",RED,"never prices on health"),("Regulator / Governance",PURP,"audit & sign-off")]
    for i,(t,c,sub) in enumerate(ents):
        a=-math.pi/2+i*2*math.pi/6; px,py=cx+330*math.cos(a),cy+250*math.sin(a)
        rect(px-150,py-38,300,76,rx=14,fill="#10182a",stroke=c,sw=1.8); text(px,py-4,t,20,c,anchor="middle",weight="bold"); text(px,py+24,sub,15,MUT,anchor="middle")
        curve(cx+95*math.cos(a),cy+95*math.sin(a),px-(110*math.cos(a) if abs(math.cos(a))>.3 else 0),py-(60*math.sin(a)),col=c,sw=1.6,op=.5,arrow=True)
    bullet(90,330,"Clinician-in-the-loop","Wellness-grade by design: every red/critical escalates to a human — no autonomous diagnosis.",GREEN)
    bullet(90,430,"Closed care loops","Lab gaps → MEASURE nudges; prescriptions → adherence; escalation → follow-up.",ACC)
    bullet(90,560,"Payer firewall","Health data & protected attributes can never worsen access or pricing (Doc 10/11).",RED)
    bullet(90,660,"Governable & auditable","Evidence registry + model-version gates + human approval on every guideline change.",PURP)
    end(n)

@slide
def s12(n):
    start(); head("11 · PATIENT JOURNEY","From first measurement to lifelong partner","One continuous loop across every entity.",CY)
    steps=[("Onboard","sex·age·ethnicity·history",ACC),("Baseline","first score + coverage",TEAL),("Daily","score · trajectory · top-5 nudges",GREEN),
           ("Early-warning","drift caught pre-threshold",CY),("Escalate","clinician review on Alert",GOLD),("Treat","therapy + adherence loop",PURP),
           ("Re-measure","outcome captured",TEAL),("Personalize","baseline tightens, score sharpens",GREEN)]
    x0,y=110,470; bw,gap=200,18
    for i,(t,sub,c) in enumerate(steps):
        x=x0+i*(bw+gap); rect(x,y,bw,150,rx=16,fill="#10182a",stroke=c,sw=1.8)
        text(x+bw/2,y+46,f"{i+1}",30,c,anchor="middle",weight="bold"); text(x+bw/2,y+88,t,23,INK,anchor="middle",weight="bold")
        text(x+bw/2,y+120,sub,14,MUT,anchor="middle")
        if i<7: line(x+bw,y+75,x+bw+gap,y+75,col=MUT,sw=2,arrow=True)
    curve(x0+7*(bw+gap)+bw/2,y+150,x0+bw/2,y+150,col=PURP,sw=2.5,dash="7 6",arrow=True,bow=130)
    text(W/2,y+300,"…and the loop repeats — every cycle the engine knows the patient better.",24,PURP,anchor="middle",ital=True)
    bullet(150,360,"Continuous, not episodic — care between visits, not just at them.",None,CY)
    end(n)

@slide
def s13(n):
    start(); head("12 · THE FLYWHEEL","How the AI doctor keeps improving","Data + reinforcement loop → compounding intelligence.",PURP,)
    cx,cy,R=1380,580,250
    nodes=[("Measure","new data point",ACC),("Personal baseline","z-score updates",TEAL),("Sharper detection","earlier, fewer false alarms",CY),
           ("Recommended action","ranked nudge / referral",GREEN),("Outcome captured","did it work?",GOLD),("Recalibrate","cohort + personal learning",PURP)]
    pos=[]
    for i in range(6):
        a=-math.pi/2+i*2*math.pi/6; pos.append((cx+R*math.cos(a),cy+R*math.sin(a)))
    for i in range(6):
        x1,y1=pos[i]; x2,y2=pos[(i+1)%6]; curve(x1,y1,x2,y2,col=PURP,sw=2.5,arrow=True,bow=-46,op=.7)
    for i,(t,sub,c) in enumerate(nodes):
        px,py=pos[i]; rect(px-118,py-40,236,80,rx=14,fill="#10182a",stroke=c,sw=1.8)
        text(px,py-6,t,20,c,anchor="middle",weight="bold"); text(px,py+24,sub,15,MUT,anchor="middle")
    circle(cx,cy,86,fill="#160f28",stroke=PURP,sw=2); text(cx,cy-6,"Compounding",19,"#fff",anchor="middle",weight="bold"); text(cx,cy+22,"intelligence",19,"#fff",anchor="middle",weight="bold")
    bullet(90,330,"Reinforcement signal","Realized ΔPureScore vs predicted trains adherence & impact models per patient.",PURP)
    bullet(90,430,"Empirical-Bayes shrinkage","Personal evidence overrides the cohort prior as it accrues — no cold-start cliff.",ACC)
    bullet(90,530,"Cohort learning","Each patient's outcomes recalibrate bands for similar patients (privacy-preserving).",TEAL)
    bullet(90,630,"Governed updates","Every recalibration is a versioned, validated, human-approved release.",GREEN)
    bullet(90,730,"The moat","More data → better personalization → better outcomes → more data.",GOLD)
    end(n)

@slide
def s14(n):
    start(); head("13 · TRUST & GOVERNANCE","Earned, not asserted","The discipline that lets a clinician rely on it.",GREEN)
    gates=[("Calibration gate","recalibrated ECE ≤ 0.05 before any risk read",GREEN),
           ("Fairness gate","parity across sex, ethnicity, life-stage (ratio ≥ 0.80)",GREEN),
           ("Early-warning gate","PPV at realistic prevalence + alarm budget",GREEN),
           ("Human-in-the-loop","every critical & every guideline change approved",ACC),
           ("Equity firewall","no protected attribute worsens score / price / access",RED),
           ("Evidence registry + crawler","citations kept current under review gate",GOLD)]
    for i,(t,sub,c) in enumerate(gates):
        col=i%2;row=i//2;x=90+col*900;y=320+row*175
        rect(x,y,840,150,rx=16,fill="#10182a",stroke=c,sw=1.8)
        text(x+28,y+58,"✓",34,c,weight="bold"); text(x+76,y+58,t,27,INK,weight="bold"); text(x+76,y+102,sub,20,MUT)
    text(90,880,"Executable validation harness runs these gates today on synthetic data — verdict: NO-SHIP until real-data proof.",22,CY,ital=True)
    end(n)

@slide
def s15(n):
    start(); head("14 · ROADMAP","From design to evidence","A credible path — honest about what remains.",GOLD)
    phases=[("NOW","Design complete","17-doc spec · live engine · evidence registry · UAE localization · validation harness",GREEN),
            ("NEXT","Prospective validation","real linked cohorts · calibrate constants · clinician sign-off",ACC),
            ("THEN","Productionize","wire full catalogue · crawler + CI · longitudinal early-warning",GOLD),
            ("SCALE","Compcounding moat","cohort learning · multi-market localization · payer/clinician integrations",PURP)]
    x0=110
    for i,(tag,t,sub,c) in enumerate(phases):
        x=x0+i*440; rect(x,360,410,360,rx=18,fill="#10182a",stroke=c,sw=1.8)
        chip(x+24,388,tag,c); text(x+30,500,t,28,INK,weight="bold")
        # wrap sub manually
        words=sub.split(" · ")
        for j,wd in enumerate(words): text(x+30,548+j*40,"· "+wd,19,MUT)
        if i<3: line(x+410,540,x+440,540,col=MUT,sw=2,arrow=True)
    text(W/2,800,"The deliverable today is a defensible design — its value is unlocked by the data + reinforcement loop over time.",24,INK,anchor="middle",ital=True)
    end(n)

# ---------------- 12-pillar drill-down ----------------
PILLARS=[
 ("CV","Cardiovascular",RED,(-1,1,-1,1),"Heart & arteries — the world's leading cause of death.",
   ["Systolic blood pressure","ApoB / atherogenic lipids","Resting heart rate"],"Atherogenic burden (λ→0: a near-permanent, decades-long memory).","A statin-controlled ApoB reads MANAGED — credited, but never ‘cured’."),
 ("MET","Metabolic",GOLD,(1,-1,1,-1),"Blood sugar & insulin handling — the UAE's defining burden.",
   ["HbA1c","Fasting glucose","Waist circumference"],"Glycemic burden — feeds CV, renal, hepatic risk.","WHO South-Asian waist cut-points catch risk a normal BMI hides."),
 ("REN","Renal",ACC,(-1,1,-1,1),"Kidney filtration & damage — silent until late.",
   ["eGFR (race-free CKD-EPI 2021)","UACR (albuminuria)","Potassium"],"Couples to glycemic & atherogenic burden.","UACR rises before eGFR falls — a true leading indicator."),
 ("HEP","Hepatic",GREEN,(1,-1,1,-1),"Liver health & fibrosis (MASLD/NAFLD).",
   ["ALT","FIB-4 score","Bilirubin"],"Hepatic-fat burden, linked to metabolic load.","FIB-4 stratifies fibrosis risk early, non-invasively."),
 ("INF","Inflammation","#e0673e",(-1,1,-1,1),"Systemic inflammatory load — a cross-cutting amplifier.",
   ["hsCRP","White cell count","Albumin"],"Inflammatory load — couples into CV, MET, hepatic, mood.","Heavy-tailed CRP is handled on a log scale to avoid false alarms."),
 ("HEM","Hematologic","#c0506b",(1,-1,1,-1),"Blood & oxygen carriage.",
   ["Hemoglobin","SpO₂","Platelets"],"Iron/oxygen reserve; hemoglobinopathy context (regional).","Hgb reference follows the hormonal milieu, not the birth certificate."),
 ("ENDO","Endocrine",PURP,(-1,1,-1,1),"Hormones, thyroid & the reproductive axis.",
   ["TSH","Cortisol (AM)","Sex hormones (T / E₂)"],"Allostatic/stress reservoir coupling.","Cycle, pregnancy, menopause, andropause & GAHT all reframe the bands."),
 ("BCM","Body Comp / MSK",TEAL,(1,-1,1,-1),"Muscle, fat distribution & bone.",
   ["BMI (ethnicity-aware)","Grip strength","BMD T-score"],"Muscle reserve — protects metabolism & independence.","Grip & VO₂max surface sarcopenic frailty before a fall."),
 ("NUT","Nutrition","#7bbf5a",(-1,1,-1,1),"Micronutrient sufficiency.",
   ["Vitamin D","Omega-3 index","Vitamin B12"],"Feeds inflammation & bone reservoirs.","Vitamin-D deficiency is near-universal in the UAE — routine repletion."),
 ("SLP","Sleep","#6a7bd6",(1,-1,1,-1),"Sleep quantity, quality & rhythm.",
   ["Duration","Efficiency","Regularity"],"Sleep debt (drains in days; the biggest hidden drag).","One sleep action improves sleep, metabolic, mood & cardiovascular at once."),
 ("FIT","Fitness","#3fb6a8",(-1,1,-1,1),"Cardiorespiratory capacity & activity.",
   ["VO₂max","Moderate-vigorous min/wk","Daily steps"],"Cardiorespiratory reserve (an asset that protects CV & MET).","VO₂max is a top mortality predictor — a β-blocker confounds it (down-weighted)."),
 ("MCS","Mental / Cognitive","#d98ec0",(1,-1,1,-1),"Mood, stress, cognition & connection.",
   ["PHQ-9 (depression)","GAD-7 (anxiety)","Loneliness (UCLA-3)"],"Allostatic load couples to sleep, endocrine & CV.","A hard safety rule: self-harm flag forces immediate crisis escalation."),
]
def make_pillar(idx):
    pid,name,col,tabs,tag,markers,res,insight=PILLARS[idx]
    def fn(n):
        start(); head(f"PILLAR {idx+1} / 12 · {pid}",name,tag,col)
        piece(1480,560,420,300,tabs,col,"",op=.20)
        text(1480,582,pid,82,"#fff",anchor="middle",weight="bold")
        bullet(90,360,"What it measures",None,col)
        for j,m in enumerate(markers): text(150,418+j*46,"• "+m,24,INK)
        bullet(90,600,"Reservoir / coupling",None,PURP); text(150,650,res,21,MUT)
        bullet(90,730,"Why it matters",None,GOLD); text(150,780,insight,21,INK,ital=True)
        # mini puzzle locator
        end(n)
    return fn
for i in range(12): slide(make_pillar(i))

@slide
def sLast(n):
    start()
    add(f'<rect x="0" y="0" width="{W}" height="{H}" fill="url(#halo)"/>')
    radial_puzzle(1430,540,300)
    chip(80,150,"SUMMARY",GOLD)
    text(80,330,"One score.",70,INK,weight="bold")
    text(80,408,"Twelve pieces. A trust layer. A loop.",46,CY,weight="bold")
    bullet(80,510,"Worst-sensitive score that can't hide a crisis",None,ACC)
    bullet(80,566,"Early-warning that catches disease incubating",None,CY)
    bullet(80,622,"Context-aware & UAE-localized, evidence-cited",None,GOLD)
    bullet(80,678,"Clinician-in-loop, equity-firewalled, auditable",None,GREEN)
    bullet(80,734,"Compounds with data — the AI doctor that improves",None,PURP)
    text(80,840,"Built on the Babylon discipline: no claim ships ahead of its evidence.",24,MUT,ital=True)
    text(80,884,"Today: a defensible design.  Next: the data that proves it.",24,INK,weight="bold")
    end(n)

TOTAL=len(SLIDES)
for i,fn in enumerate(SLIDES,1): fn(i)
# assemble PDF (manual FlateDecode writer — no JPEG dependency)
imgs=[Image.open(p).convert("RGB") for p in paths]
pdf=os.path.join(OUT,"purescore-executive-deck.pdf")
images_to_pdf(imgs,pdf)
print(f"Rendered {TOTAL} slides → {pdf}  ({os.path.getsize(pdf)//1024} KB)")
