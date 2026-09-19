// docs/m3-contracts.md section 4.6: `way(pivot.a)` -- Loring Park, a named
// leisure=park near downtown Minneapolis, is the kind of area whose pivot
// is a plain closed way (not a relation): no bbox placeholder needed, the
// area name scopes the lookup.
[out:json][timeout:25];
area[name="Loring Park"]->.a;
way(pivot.a);
out;
