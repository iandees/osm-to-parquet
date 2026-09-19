// docs/m3-contracts.md section 4.6: `area[...]` + `node[...](area.a)`.
[out:json][timeout:25];
area[name="Minneapolis"]->.a;
node[amenity=cafe](area.a);
out;
