// docs/m3-contracts.md section 4.6: `map_to_area`.
[out:json][bbox:{{bbox}}][timeout:25];
way["leisure"="park"]["name"="Loring Park"]->.w;
.w map_to_area->.a;
.a out;
