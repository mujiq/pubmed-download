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
groups. A clean run prints `Generated N pages → …` with no errors (currently 39 pages).
