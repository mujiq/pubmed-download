# Wiki — editing guide (read before changing anything here)

This directory is the **PureScore Tech Wiki**. The `*.html` pages are **generated**, not
hand-authored. Edits made directly to generated `.html` get **silently wiped on the next
rebuild** (this has already happened once — a "Visuals" menu added to the generated HTML
was lost when the wiki was rebuilt between sessions).

## Source of truth
- **Generator:** `build_wiki.py` (uses `wiki_content.py` and `wiki_admin.py`).
- **Doc page content:** Markdown in `../purescore/*.md`.
- **Custom page content:** either a Python builder in `wiki_content.py`
  (e.g. `build_behemoth`) **or** a `*.body.html` fragment wrapped by the `page()` loop in
  `build_wiki.py` (e.g. `feedback-loop.body.html`, `states.body.html`,
  `engagement-state-machines.body.html`). Edit the **fragment / builder**, never the
  generated `.html`.
- **Standalone full pages** (their own `<html>`, not wiki chrome) such as
  `wearable-baselines.html` are **not generated** — they exist as-is and are only *linked*
  from the nav. Do not regenerate or overwrite them.

## To add or change a page / menu
1. Edit `NAV` (sidebar groups), `ORDER` (prev/next sequence), and `PTITLE` (titles) in
   `build_wiki.py`.
2. Add the content: a `../purescore/*.md`, a `wiki_content.py` builder, or a
   `*.body.html` fragment registered in the `page()` loop.
3. Run `python3 build_wiki.py` from this directory — it regenerates **all** pages, so the
   sidebar/nav propagates everywhere.

## Multi-agent coordination
`build_wiki.py` is edited by multiple agents and changes between sessions. **Always re-read
`NAV` / `ORDER` / `PTITLE` immediately before editing** and integrate *alongside* existing
groups rather than overwriting — e.g. a "Whole-system map → Behemoth Class Diagram" group
appeared between tasks and had to be preserved when adding the "State Diagram" and "Visuals"
groups. A clean run prints `Generated N pages → …` with no errors.

## Diagrams — keep in sync, evaluate impact on every page change (STANDING RULE)
Diagrams are **derived artifacts**. On **any** change to a doc, `data/*.json`, or a builder,
you **must**: (1) consult the dependency map below, (2) update every affected diagram, and
(3) state the diagram impact in your summary (what you reviewed, what changed, what stayed).
Do not let a spec change silently desync a diagram. If unsure whether a diagram is affected,
open it and check.

**Engine is JSON-canonical (D32).** The scoring math is NOT hardcoded in any page. `data/calc-graph.json` (+ `data/pillar-weights.json`, `data/constants.json`) is canonical; `build_wiki.py` calls `wiki_content.write_calc_data()` to resolve it into the generated `assets/calc-data.js`, which the shared `assets/engine.js` consumes. **Change scoring by editing the JSON, then rebuild — never edit a page's JS scorer.** `build_wiki.py:_engine_guard()` HARD-FAILS the build if weights don't sum to 1, calc-graph refs don't resolve, `calc-data.js` is stale, or any page reintroduces a hardcoded scorer (`var TH={`, `var PILLMETA=`, `function scoreProfile`, `var PILL={cv:`).

**IA note (restructure):** docs renumbered **01–19 to reading order**; the menu is **9 chapters** (Start here + 8, incl. a 'Gaps, blind-spots & roadmap' chapter); each appendix catalog now embeds its sortable **"Spreadsheet view"** so there are **no standalone `grid-*` pages**; the static **DFD was folded into the Calculation Explorer**; **male+female → `purescore-sex.html`** (segmented toggle) and **class diagram + class explorer → `class-model.html`** (`build_class_model` wraps `build_behemoth` + `build_class_explorer`).

**Diagram → source dependency map**
- `purescore-uber-map.html` (**Calculation Explorer**: audit Tree + flow Map + full decision tree, 5 roots, profiles, editable leaves; also serves as the system dataflow view) ← `data/calc-graph.json` via `assets/engine.js` + `assets/calc-explorer.js`. *Any scoring/gate/pillar/reservoir/profile change → edit `calc-graph.json` (+ catalogs) and rebuild; do NOT hand-edit node/edge/scorer JS.*
- `assets/engine.js` (shared scorer) + `assets/calc-explorer.js` (Tree/Map/drawer UI) ← `window.PURESCORE_DATA` (generated `assets/calc-data.js`). `feedback-loop.body.html` keeps its own Stage-2b temporal demo but sources weights/γ/δ from the same `PURESCORE_DATA`.
- `class-model.html` (**Class diagram + Class explorer**, merged) ← the whole object model (Docs 02, 03, 04, 05, 06, 11) — data-driven in `wiki_content.py` (`build_class_model`, wrapping `build_behemoth` + `build_class_explorer`); also see the `[[behemoth-diagram-keep-in-sync]]` memory.
- `engagement-state-machines.html` ← Doc 11 (nudge), Appendix adherence (`data/adherence.json`), Docs 13/16/05.
- `states.html` (patient life-state machine) ← Docs 08/09 (acute, life-stage), data streams (Doc 07).
- `index.html` "How the documents connect" flow ← the doc set / NAV.
- `appendix-coverage-audit.html` loop diagram ← `data/question-bank.json`, `data/adherence.json`.
- `questions-hub.html` / `eligibility-gating.html` diagrams ← `data/question-bank.json` (sections/phases/gates).
- Per-doc inline `<pre class="mermaid">` (in `C.MERMAID`) ← that doc's content.

A clean run prints `Generated N pages → …` with no errors and **no `! skip` lines**.
