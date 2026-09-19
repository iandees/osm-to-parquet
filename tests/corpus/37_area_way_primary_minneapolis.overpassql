// docs/m3-contracts.md section 4.6: `way[...](area.a)`.
[out:json][timeout:25];
area[name="Minneapolis"]->.a;
way[highway=primary](area.a);
out;
