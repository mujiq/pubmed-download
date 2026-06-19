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

**Diagram → source dependency map**
- `purescore-dataflow.html` (PureScore Calculation DFD) ← Docs 01, 02, 03, 04, 05, 06, 11, 12 + admin weights/δ. *Any scoring/gate/reservoir/critical-marker/acute/coverage change → review here first.*
- `purescore-uber-map.html` (interactive full-lifecycle map + 10 sample profiles) ← the **entire pipeline**: Docs 01–04 (scoring/reservoirs), 06/07/11/12 (lifecycle), Appendix H (adherence), eligibility/onboarding. *Any scoring, gate, pillar, reservoir, lifecycle, or profile change → update this map's node/edge data + profile scorer too.*
- `behemoth-class-diagram.html` ← the whole object model (Docs 01–04, 07, 12) — data-driven in `wiki_content.py` (`build_behemoth`); also see the `[[behemoth-diagram-keep-in-sync]]` memory.
- `engagement-state-machines.html` ← Doc 07 (nudge), Appendix H adherence (`data/adherence.json`), Docs 09/11/12.
- `states.html` (patient life-state machine) ← Docs 05/06 (acute, life-stage), data streams (Doc 18).
- `index.html` "How the documents connect" flow ← the doc set / NAV.
- `appendix-coverage-audit.html` loop diagram ← `data/question-bank.json`, `data/adherence.json`.
- `questions-hub.html` / `eligibility-gating.html` diagrams ← `data/question-bank.json` (sections/phases/gates).
- Per-doc inline `<pre class="mermaid">` (in `C.MERMAID`) ← that doc's content.

A clean run prints `Generated N pages → …` with no errors and **no `! skip` lines**.
