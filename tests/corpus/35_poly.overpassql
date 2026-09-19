// A small rectangle around the East Bank/Stadium Village area of
// Minneapolis, containing exactly one amenity=cafe node ("Mu Mu Tea",
// 6332252021) -- verified at the reference server. Literal polygon: no
// bbox placeholder needed.
[out:json][timeout:25];
node[amenity=cafe](poly:"44.9727 -93.2238 44.9740 -93.2238 44.9740 -93.2228 44.9727 -93.2228");
out;
