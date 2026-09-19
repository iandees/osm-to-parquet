// bbox: tiny (tests/corpus/bboxes.json) -- hardcoded, not {{bbox}}, so this
// entry always runs against the small bbox it was picked for regardless of
// the harness's default bbox selection for a query with no {{bbox}}.
[out:json][timeout:25][date:"2026-09-19T06:00:00Z"];
node["amenity"](44.9770,-93.2700,44.9800,-93.2670);
out tags;
