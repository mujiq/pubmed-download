# PureScore — Technical Wiki (`docs/tech/`)

A navigable, interlinked HTML rendering of the full PureScore design specification
(`docs/purescore/*.md`), plus reference appendices, the Doctor's-board admin screens, and a live
feedback-loop demo.

**Open `index.html` in a browser** to read it. (Mermaid diagrams render via CDN, so live diagrams
need internet; the diagram source is shown as a fallback when offline. Everything else works offline.)

## What's here
| Page(s) | Source | Contents |
|---|---|---|
| `index.html` | generated | Landing: doc map, “how the documents connect” diagram, all entry points |
| `NN-*.html` (00–18) | `docs/purescore/NN-*.md` | Each spec doc, rendered, with a per-doc diagram + cross-links |
| `conventions.html` | `README.md` | Binding conventions, the 12 pillars, glossary, safety non-negotiables |
| `decisions.html` | `decisions.md` | The full decision log (D1–D23) |
| `appendix-biomarkers.html` | Doc 02 | Exhaustive marker catalogue — bands, tiers, weights, sources |
| `appendix-wearables.html` | Doc 18 / 01 | Wearable metrics, layers, trust tiers (D22), `q_source` |
| `appendix-questions.html` | Doc 02 / 18 | PHQ-9, GAD-7, AUDIT-C, ISI, WHO-5… full item text, scoring, cut-offs |
| `appendix-personas.html` | Doc 05 / 15 + calculator | Cohort frames & edge-case personas |
| `admin-*.html` | Docs 02/03/11/13/14 | Doctor's-board quarterly review screens (high-fidelity static mockups) |
| `feedback-loop.html` | D23 / Doc 03 §2b | Interactive continuous-scoring demo |

## Regenerate
```bash
cd docs/tech
python3 build_wiki.py
```
- `build_wiki.py` — markdown→HTML converter, page template, cross-link resolver, nav/prev-next, index.
- `wiki_content.py` — reference data (biomarkers, wearables, instruments, personas, weights) + appendix builders.
- `wiki_admin.py` — Doctor's-board admin screen builders.
- `feedback-loop.body.html` — the interactive demo body (wrapped by the generator).
- `assets/wiki.css`, `assets/wiki.js` — shared theme + navigation/search.

No third-party Python packages required (stdlib only). All clinical values are **illustrative and
must be re-verified before production** (`README.md` §5.6).
