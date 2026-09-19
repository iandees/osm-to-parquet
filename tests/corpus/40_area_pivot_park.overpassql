// docs/m3-contracts.md section 4.6: `way(pivot.a)` -- Loring Park, a named
// leisure=park near downtown Minneapolis, is the kind of area whose pivot
// is a plain closed way (not a relation). The pivot query carries a
// Minneapolis bbox because the reference is worldwide and other parks
// share the name.
[out:json][timeout:25];
area[name="Loring Park"]->.a;
way(pivot.a)(44.9,-93.4,45.1,-93.2);
out;
