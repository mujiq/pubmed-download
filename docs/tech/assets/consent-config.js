/* PureScore consent — deployment configuration.
   Ops/legal fill these in once; the consent page reads them at runtime so the page text AND the
   immutable audit record carry real values instead of {{placeholders}}.
   - controller: the legal data-controller name shown to the patient and stored in the record.
   - version: the consent-document version stamped into every saved record.
   - auditEndpoint: POST target for the consent audit log. Leave EMPTY to run as a static page
     (the record is still stored in localStorage; nothing is sent).
   When a field is left blank the page falls back to its visible {{TOKEN}} placeholder, so an
   unconfigured deployment is obviously a draft rather than silently shipping a wrong value. */
window.PURESCORE_CONSENT = {
  controller: "",
  version: "v1.0-draft",
  auditEndpoint: ""
};
