// docs/m3-contracts.md section 4.6: `rel[...](area.a)`.
[out:json][timeout:25];
area[name="Minneapolis"]->.a;
rel[route=bus](area.a);
out;
